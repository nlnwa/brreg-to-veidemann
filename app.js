/**
 *
 * The script creates a .json file of objects to insert as entities and seeds in Veidemann.
 *
 * @param source
 * The scripts uses a file  JSON array of organizations from brreg.no (https://data.brreg.no/enhetsregisteret/api/enheter/lastned)
 * and locates every organization object that has specified a homepage that matches certain requirements and writes that object to a
 * new file (brregData)
 * @param brregData
 * JSON file containing Organization objects with a homepage
 *
 * @function createImportList
 * The function uses the brregData file to creat new file with JSON objects to be imported as entities and seeds in Veidemann.
 *
 * @output
 *
 * output - the import file to be used with veidemannctl
 * brregData - file with organization objects with homepage, used for creating the output file.
 * errorStream - file with urls that didn't meet our requirements.
 * createImportList.errorStream -  file with invalid urls
 *
 */


const StreamArray = require('stream-json/streamers/StreamArray');
const path = require('path');
const fs = require('fs');
const {createImportList} = require('./veidemann_import');
const inputDir = './input';
const outputDir = './output';

if (!fs.existsSync(inputDir)) {
    fs.mkdirSync(inputDir);
}

if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir);
}


const source = 'brregOrganizations.json';
const brregData = 'updated.json';
const output = 'brreg_import.json';
const nonMatchUrl = 'nonmatchedurls.txt';

function processBrreg() {
    const inputStream = StreamArray.withParser();

    fs.createReadStream(path.join(__dirname, inputDir, source)).pipe(inputStream.input);
    const outputStream = fs.createWriteStream(path.join(__dirname, outputDir, brregData));
    const errorStream = fs.createWriteStream(path.join(__dirname, outputDir, nonMatchUrl));

    const t0 = Date.now();

    //const urlRegEx = /([æøåa-z]|[ÆØÅA-Z]|[0-9]|\.|http\:\/\/|https\:\/\/|ws\:\/\/|wss\:\/\/)+\.((com|edu|gov|mil|net|org|biz|info|name|museum|gob|no|as)|(com|no|as|edu|gov|mil|net|org|biz|info|name|museum|gob)\.([æøåa-z]|[ÆØÅA-Z])([a-z]|[A-Z]))(?=(\s|$|\/|\[|\]|\{|\}|\(|\)|\,|\;|\'|\"|\||\t|\n|\r))/;
    const urlRegEx = /([æøåa-z]|[ÆØÅA-Z]|[0-9]|\.|http\:\/\/|https\:\/\/|ws\:\/\/|wss\:\/\/)+\.((no)|(no)\.([æøåa-z]|[ÆØÅA-Z])([a-z]|[A-Z]))(?=(\s|$|\/|\[|\]|\{|\}|\(|\)|\,|\;|\'|\"|\||\t|\n|\r))/;
    const fbRegEx = /^((?!facebook).)*$/;

    let entitiesChecked = 0;
    let nonEmptyWebPageFields = 0;
    let validWebPages = 0;
    let validWebPageNotFacebook = 0;
    let urlFacebook = 0;
    let lastPrint = 0;

    inputStream.on('data', ({key, value}) => {
        const separator = "\n";
        entitiesChecked++;
        if (lastPrint + 10000 === entitiesChecked) {
            lastPrint = entitiesChecked;
            console.log('Har sjekket ', lastPrint, ' entiteter fra brreg');
        }

        if (value.hjemmeside) {
            nonEmptyWebPageFields++;

            if (urlRegEx.test(value.hjemmeside.toLowerCase())) {
                validWebPages++;
                if (fbRegEx.test(value.hjemmeside.toLowerCase())) {
                    validWebPageNotFacebook++;
                    outputStream.write(JSON.stringify(value) + separator);
                } else {
                    urlFacebook++;
                }
            } else {
                errorStream.write(JSON.stringify(value.hjemmeside) + '\n');
            }
        }
    });

    inputStream.on('end', () => {
        outputStream.end();
        errorStream.end();

        const t1 = Date.now();
        const duration = (t1 - t0) / 1000;
        console.log('***********************************************************************');
        console.log('Har sjekket: ', entitiesChecked, ' enheter fra brreg');
        console.log('Av disse har: ', nonEmptyWebPageFields, ' feltet hjemmeside utfylt');
        console.log('Av disse igjen ser: ', validWebPages, ' ut som de har en gyldig url');
        console.log('Av disse er det: ', validWebPageNotFacebook, ' som ikke er mot facebook');
        console.log('Og har dermed: ', urlFacebook, ' URIer som går til facebook');
        console.log('***********************************************************************');
        console.log('Operasjonen tok: ', duration, ' sekunder å gjennomføre');
        console.log('Starter opprettingen av liste til bruk med veidemannctl');

        createImportList(path.join(__dirname, outputDir, brregData),path.join(__dirname, outputDir, output));
   });
}

processBrreg();

