const fs = require('fs')
const parse = require('csv-parse')
const path = require('path')
const transform = require('stream-transform')
const moment = require('moment')

const currencyRates = require('./curr-conv')()

let dataObject = {
    date: undefined,
    currencies: {},
    total: 0
}

let indir = 'data'
const datFile = fs.createReadStream(path.resolve(indir, 'OECOLB_20190222_00000055.DAT'))
datFile.on('error', (err) => { console.error(err.message); process.exit(1) })
datFile.on('end', () => console.log(`${JSON.stringify(dataObject, {}, 2)}`))

const parser = parse({ delimiter: '|', relax_column_count: true })

const amount = transform((line) => {
    const currency = line[21]
    const amount =  parseFloat(line[16])
    const converted = currency !== 'EUR' ? amount / currencyRates.rates[currency] : amount
    if (dataObject.currencies[currency]) {
        dataObject.currencies[currency].original = dataObject.currencies[currency].original + amount
        dataObject.currencies[currency].converted = dataObject.currencies[currency].converted + converted
    } else {
        dataObject.currencies[currency] = {
            original: amount,
            converted
        }
    }
    dataObject.total = dataObject.total = dataObject.total + converted
    return [currency, amount]
})
const getDate = transform((line) => line[0] === 'H' ? dataObject.date = moment(line[2], 'YYYYMMDD') : line)
const removeHeaderAndFooter = transform((line) => line[0] === 'D' ? line : undefined)

// need to keep the semicolon!
;(async () => {
    await new Promise((resolve, reject) => {
        datFile
            .pipe(parser)
            .pipe(getDate)
            .pipe(removeHeaderAndFooter)
            .pipe(amount)
            .on('end', () => {
                resolve(dataObject)
            })
    })
})()
