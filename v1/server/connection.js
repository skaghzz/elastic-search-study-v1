const elasticsearch = require('elasticsearch')
const index = 'library'
const type = 'novel'
const port = 9200
const host = process.env.ES_HOST || 'localhost'
const client = new elasticsearch.Client({ host: { host, port } })

async function checkConnection() {
    let isConnected = false
    while (!isConnected) {
        console.log('Connection to ES')
        try {
            const health = await client.cluster.health({})
            console.log(health)
            isConnected = true
        } catch (err) {
            console.log('connection failed, retrying...', err)
        }
    }
}

checkConnection()

async function resetIndex() {
    if (await client.indices.exists({ index })) {
        await client.indices.delete({ index })
    }
    await client.indices.create({ index })
    await putBookMapping()
}

/** add book section schema mapping to ES */
async function putBookMapping() {
    const schema = 
    {
        //type : {
            'title': {'type': 'keyword'},
            'author' : {'type': 'keyword'},
            'location' : {'type':'integer'},
            'text' : {'type':'text'}
        //}
    }

    return client.indices.putMapping({ index: index, type: type, body: { properties: schema }, include_type_name:true })
}

module.exports = {
    client, index, type, checkConnection, resetIndex
}