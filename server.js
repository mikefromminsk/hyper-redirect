import express from 'express'
import Corestore from 'corestore'
import Hyperswarm from 'hyperswarm'
import crypto from 'crypto'
import {Level} from 'level'
import fs from 'fs'

let logs = []

function log(data) {
    console.log('RCORE:', data)
    logs.push('' + data)
    if (logs.length > 10)
        logs.shift()
}

process.on('unhandledRejection', log)
process.on('uncaughtException', log)

const configFilename = './config.json'

let config = {
    API_PORT: 80,
    P2P_SEED: crypto.randomBytes(32).toString('hex'),
    P2P_LOCAL_KEY: '',
    P2P_ROOM: '',
    P2P_REMOTE_KEY: '',
    P2P_PORT: 59737,
    P2P_REDIRECT: 'http://localhost:8999',
    P2P_MAX_STORE: 100,
    ...fs.existsSync(configFilename) ? JSON.parse(fs.readFileSync(configFilename, 'utf-8')) : {}
}

const put = (key, value) => {
    config[key] = value
    log(key + ': ' + value)
    fs.writeFileSync(configFilename, JSON.stringify(config, null, 2))
}

for (const key in config)
    if (process.env[key])
        put(key, process.env[key])

const is_key = (s) => /^[0-9a-f]{64}$/i.test(s)

const remoteKeys = []
for (const key of config.P2P_REMOTE_KEY.split(',')) {
    if (!is_key(key)) {
        log('BAD REMOTE KEY: ' + key)
    } else {
        remoteKeys.push(key)
    }
}

const level = new Level('./level')

const read_int = async (key, fallback = 0) => {
    try { return parseInt(await level.get(key), 10) } catch { return fallback }
}

const corestore = new Corestore('./corestore', {
    primaryKey: crypto.createHash('sha256').update(config.P2P_SEED).digest(),
    unsafe: true
})

let swarm = null
let localCore = null
let remoteCores = []
let local_last = null
let redirecting = false


const app = express()
app.use(express.json({limit: 1024 * 1024}))

app.post('/append', async (req, res) => {
    try {
        if (!req.body.stamp)
            req.body.stamp = crypto.randomInt(100_000_000_000, 1_000_000_000_000)

        const record = JSON.stringify(req.body)

        if ((localCore?.byteLength || 0) + Buffer.byteLength(record) > config.P2P_MAX_STORE * 1024 * 1024)
            throw new Error(`Store limit reached: ${config.P2P_MAX_STORE} MB`)

        if (await read_int('local_stamp_' + req.body.stamp, 0) === 1)
            return res.json({stamp: req.body.stamp})

        await level.put('local_stamp_' + req.body.stamp, '1')
        await localCore.append(record)
        return res.json({stamp: req.body.stamp})
    } catch (e) {
        log(e.message)
        return res.status(500).json({error: e.message})
    }
})

async function remoteRedirect() {
    if (redirecting) return
    redirecting = true
    try {
        for (const remote of remoteCores) {
            while (remote.pos < remote.core.length) {
                let body = await remote.core.get(remote.pos)
                let request = JSON.parse(body)

                if (await read_int('local_stamp_' + request.stamp, 0) === 1) {
                    await level.put('remote_pos_' + remote.key, String(++remote.pos))
                    continue
                }

                local_last = request
                try {
                    const res = await fetch(config.P2P_REDIRECT, {
                        method: 'POST',
                        body: body,
                        headers: {'Content-Type': 'application/json'}
                    })
                    log('redirect ' + (res.ok ? 'success: ' : 'error: ') + await res.text())
                    if (res.ok) {
                        await level.put('remote_pos_' + remote.key, String(++remote.pos))
                        await remote.core.clear(remote.pos - 1, remote.pos)
                        continue
                    }
                } catch (e) {
                    log('redirect error: ' + e.message)
                }
                break
            }
        }
    } finally {
        redirecting = false
    }
}

async function redirectJob() {
    await remoteRedirect()
    setTimeout(() => redirectJob(), 5000)
}


app.get('/debug',async (req, res) => {
    res.json({
        ...config,
        room: room,
        local_length: localCore?.length || 0,
        remote_length: remoteCores[0]?.core.length || 0,
        remotes: remoteCores.map(r => ({key: r.key, length: r.core.length, pos: r.pos})),
        local_last: local_last,
        swarm_pubkey: swarm?.keyPair.publicKey.toString('hex') || '',
        swarm_peers: Array.from(swarm?.connections || []).map(s => s.remotePublicKey.toString('hex')),
        store_used_mb: (localCore?.byteLength || 0) / 1024 / 1024,
        logs: logs
    })
})

app.post('/config', async (req, res) => {
    const changed = []
    for (const key in req.body) {
        if (config[key] === req.body[key]) continue
        put(key, req.body[key])
        changed.push(key)
    }

    res.json({changed})

    if (changed.length) {
        log('config changed, restart')
        setTimeout(() => process.exit(0), 100)
    }
})

app.listen(config.API_PORT)
    .on('listening', () => log('SERVER START ON PORT ' + config.API_PORT))
    .on('error', (err) => log('SERVER ERROR ' + err.message))

let room = ''

async function initLocalCore() {
    localCore = corestore.get({name: 'local'})
    await localCore.ready()
    put('P2P_LOCAL_KEY', localCore.key.toString('hex'))
    room = is_key(config.P2P_ROOM) ? config.P2P_ROOM : localCore.key.toString('hex')
}

async function initRemoteCores() {
    remoteCores = remoteKeys.map(key => ({key, core: corestore.get({key: Buffer.from(key, 'hex')}), pos: 0}))
    await Promise.all(remoteCores.map(async (remote) => {
        await remote.core.ready()
        await remote.core.update()
        remote.pos = await read_int('remote_pos_' + remote.key, 0)
        log('remote core: ' + remote.key)
    }))

    if (remoteCores.length && config.P2P_REDIRECT) {
        redirectJob()
    }
}

async function initNetwork() {
    swarm = new Hyperswarm({port: parseInt(config.P2P_PORT)})
    swarm.on('connection', (socket) => corestore.replicate(socket))
    log('room: ' + room)
    swarm.join(Buffer.from(room, 'hex'), {client: true, server: true})
}

async function initCore() {
    await initLocalCore()
    await initNetwork()
    await initRemoteCores()
}

initCore()
