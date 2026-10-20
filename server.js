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

const loadConfig = () => {
    try {
        return fs.existsSync(configFilename) ? JSON.parse(fs.readFileSync(configFilename, 'utf-8')) : {}
    } catch (e) {
        log('config read error: ' + e.message)
        return {}
    }
}

let config = {
    API_PORT: 80,
    P2P_SEED: crypto.randomBytes(32).toString('hex'),
    P2P_LOCAL_KEY: '',
    P2P_ROOM: '',
    P2P_REMOTE_KEY: '',
    P2P_PORT: 59737,
    P2P_REDIRECT: 'http://localhost:8999',
    ...loadConfig()
}
delete config.remote_redirected

const save = () => {
    try {
        fs.writeFileSync(configFilename, JSON.stringify(config, null, 2))
    } catch (e) {
        log('config save error: ' + e.message)
    }
}

const put = (key, value) => {
    config[key] = value
    log(key + ': ' + value)
    save()
}

for (const key in config)
    if (process.env[key] !== undefined)
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

const advance = async (remote) => {
    remote.pos++
    await level.put('remote_pos_' + remote.key, String(remote.pos))
}


const app = express()
app.use(express.json())

app.post('/append', async (req, res) => {
    try {
        if (!req.body.stamp)
            req.body.stamp = crypto.randomInt(100_000_000_000, 1_000_000_000_000)

        if (await read_int('local_stamp_' + req.body.stamp, 0) === 1) {
            res.json({stamp: req.body.stamp})
            return
        }

        await level.put('local_stamp_' + req.body.stamp, '1')
        await localCore.append(JSON.stringify(req.body))
        res.json({stamp: req.body.stamp})
    } catch (e) {
        log(e.message)
        res.status(400).end()
    }
})

async function remoteRedirect() {
    if (redirecting) return
    redirecting = true
    try {
        for (const remote of remoteCores) {
            while (remote.pos < remote.core.length) {
                let body = await remote.core.get(remote.pos)
                let request = JSON.parse(body.toString())

                if (await read_int('local_stamp_' + request.stamp, 0) === 1) {
                    await advance(remote)
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
                        await advance(remote)
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


const debug = async (req, res) => {
    res.json({
        ...config,
        room: room,
        local_length: localCore?.length || 0,
        remote_length: remoteCores[0]?.core.length || 0,
        remotes: remoteCores.map(r => ({key: r.key, length: r.core.length, pos: r.pos})),
        local_last: local_last,
        swarm_pubkey: swarm?.keyPair.publicKey.toString('hex') || '',
        swarm_peers: Array.from(swarm?.connections || []).map(s => s.remotePublicKey.toString('hex')),
        logs: logs
    })
}

app.get('/debug', debug)
app.post('/debug', debug)

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
