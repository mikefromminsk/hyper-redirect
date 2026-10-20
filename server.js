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
    P2P_REMOTE_KEY: '',
    P2P_PORT: 59737,
    P2P_REDIRECT: 'http://localhost:8999',
    remote_redirected: 0,
    ...fs.existsSync(configFilename) ? JSON.parse(fs.readFileSync(configFilename, 'utf-8')) : {}
}

const save = () => fs.writeFileSync(configFilename, JSON.stringify(config, null, 2))

const put = (key, value) => {
    config[key] = value
    log(key + ': ' + value)
    save()
}

for (const key in config)
    if (process.env[key] !== undefined)
        put(key, process.env[key])

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
let remoteCore = null
let local_last = null


const app = express()
app.use(express.json())

app.post('/append', async (req, res) => {
    try {
        if (req.body.stamp) {
            try {
                let body = await remoteCore.get(config.remote_redirected)
                let request = JSON.parse(body.toString())

                if (req.body.stamp === request.stamp) {
                    config.remote_redirected++
                    save()
                    remoteRedirect()
                }
            } catch (e) {
                log(e.message)
            }
        } else {
            req.body.stamp = crypto.randomInt(100_000_000_000, 1_000_000_000_000)
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
    while (config.remote_redirected < remoteCore.length) {
        let body = await remoteCore.get(config.remote_redirected)
        let request = JSON.parse(body.toString())

        if (await read_int('local_stamp_' + request.stamp, 0) === 1) {
            config.remote_redirected++
            save()
            continue
        }

        local_last = request
        try {
            const res = await fetch(config.P2P_REDIRECT, {method: 'POST', body: body})
            log('redirect ' + (res.ok ? 'success: ' : 'error: ') + await res.text())
            if (res.ok) {
                config.remote_redirected++
                save()
                continue
            }
        } catch (e) {
            log('redirect error: ' + e.message)
        }
        return
    }
}

async function redirectJob() {
    await remoteRedirect()
    setTimeout(() => redirectJob(), 5000)
}


app.post('/debug', async (req, res) => {
    res.json({
        ...config,
        local_length: localCore?.length || 0,
        remote_length: remoteCore?.length || 0,
        local_last: local_last,
        swarm_pubkey: swarm?.keyPair.publicKey.toString('hex') || '',
        swarm_peers: Array.from(swarm?.connections || []).map(s => s.remotePublicKey.toString('hex')),
        logs: logs
    })
})

app.listen(config.API_PORT)
    .on('listening', () => log('SERVER START ON PORT ' + config.API_PORT))
    .on('error', (err) => log('SERVER ERROR ' + err.message))

async function initDatabaseLog() {
    localCore = corestore.get({name: 'local'})
    await localCore.ready()
    put('P2P_LOCAL_KEY', localCore.key.toString('hex'))

    if (config.P2P_REMOTE_KEY && config.P2P_REDIRECT) {
        remoteCore = corestore.get({key: Buffer.from(config.P2P_REMOTE_KEY, 'hex')})
        await remoteCore.ready()
        await remoteCore.update()
        redirectJob()
    }
}

async function initNetwork() {
    swarm = new Hyperswarm({
        localDiscovery: true,
        port: parseInt(config.P2P_PORT)
    })
    swarm.on('connection', (socket) => corestore.replicate(socket))
    if (config.P2P_REMOTE_KEY)
        swarm.join(Buffer.from(config.P2P_REMOTE_KEY, 'hex'), {client: true, server: true})
}

async function initCore() {
    await initNetwork()
    await initDatabaseLog()
}

initCore()
