const fs = require('fs')
const path = require('path')
const esConnection = require('./connection')

async function readAndInsertBooks() {
    try {
        await esConnection.resetIndex()

        let files = fs.readdirSync('./books').filter(file => file.slice(-4) === '.txt')
        console.log('Found ${files.length} Files')

        for (let file of files) {
            console.log(`Reading file - ${file}`)
            const filePath = path.join('./books', file)
            const { title, author, paragraphs } = parseBookFile(filePath)
            await insertBookData(title, author, paragraphs)
        }
    } catch (err) {
        //console.err(err)
        console.error(err)
    }
}

/** Read an individual book text file, and extract the title, author and paragraph */
function parseBookFile(filePath) {
    // Read text file
    const book = fs.readFileSync(filePath, 'utf8')

    const title = book.match(/^Title:\s(.+)$/m)[1]
    const authorMatch = book.match(/^Author:\s(.+)$/m)
    const author = (!authorMatch || authorMatch[1].trim() === '') ? 'Unknown Author' : authorMatch[1]

    console.log(`Reading Book - ${title} By ${author}`)

    // Find Guttenberg metadata header and footer
    const startOfBookMatch = book.match(/^\*{3}\s*START OF (THIS|THE) PROJECT GUTENBERG EBOOK.+\*{3}$/m)
    const startOfBookIndex = startOfBookMatch.index + startOfBookMatch[0].length
    const endOfBookIndex = book.match(/^\*{3}\s*END OF (THIS|THE) PROJECT GUTENBERG EBOOK.+\*{3}$/m).index

    const paragraphs = book.slice(startOfBookIndex, endOfBookIndex) // head랑 footer 자름
        .split(/\n\s+\n/g)  // 각 paragraph를 array로 나눔
        .map(line => line.replace(/\r\n/g, ' ').trim()) // 문단 줄넘김을 공란으로 변경
        .map(line => line.replace(/_/g, ''))    // _ 문자가 이탈릭체를 의미하므로 제거
        .filter((line) => (line && line !== ''))    // 빈줄 제거

    console.log('Parsed ${paragraphs.length} Paragraphs\n')
    return { title, author, paragraphs }
}

async function insertBookData( title, author, paragraphs) {
    let bulkOps = []

    for(let i = 0; i < paragraphs.length; i++){
        bulkOps.push({index: {_index:esConnection.index, _type: esConnection.type } })
        bulkOps.push({
            author, title, location: i, text: paragraphs[i]
        })

        if( i > 0 && i % 500 === 0) {
            await esConnection.client.bulk({ body: bulkOps })
            bulkOps = []
            console.log(`Indexed Paragraph ${i-499} - ${i}`)
        }
    }
    await esConnection.client.bulk({body: bulkOps})
    console.log(`Indexed paragraphs ${paragraphs.length - (bulkOps.length /2)} - ${paragraphs.length}\n\n\n`)
}

readAndInsertBooks()