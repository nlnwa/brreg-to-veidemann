const StreamValues = require('stream-json/streamers/StreamValues');
const fs = require('fs');
const {parse} = require('url');

/**
 * @typedef {{
 *     navn: string,
 *     naeringskode1: {
 *         kode: string
 *     },
 *     naeringskode2: *,
 *     hjemmeside: string,
 *     organisasjonsnummer: string,
 *     organisasjonsform: {
 *         beskrivelse: string,
 *         kode: string
 *     },
 *     forretningsadresse: {
 *         kommunenummer: string
 *     },
 *     institusjonellSektorkode: {
 *         kode: string
 *     },
 *     maalform: string
 * }} Brreg
 *
 * @typedef {{
 *   entityLabel: {value: string, key: string, [description]: string}[],
 *   entityDescription: string,
 *   entityName: string,
 *   uri: string,
 *   seedDescription: string
 *   seedLabel: {value: string, key: string, [description]: string}[],
 * }} Veidemannctl
 *
 */

function createImportList(input, output) {

    const inputStream = StreamValues.withParser();

    fs.createReadStream(input).pipe(inputStream.input);
    const outputStream = fs.createWriteStream(output);

    const errorStream = fs.createWriteStream('./output/failedurlcheck.txt');
    const t0 = Date.now();
    let count = 0;

    inputStream.on('data', ({key, value}) => {
        count += 1;
        const veidemannSeed = transform(value);
        if (veidemannSeed.entityName && veidemannSeed.uri) {
            outputStream.write(JSON.stringify(veidemannSeed) + '\n');
        } else {
            errorStream.write(JSON.stringify(veidemannSeed) + '\n');
        }

    });

    inputStream.on('end', () => {
        outputStream.end();
        errorStream.end();
        const t1 = Date.now();

        console.log('***********************************************************************');
        console.log('Har sjekket : ', count, ' entiteter fra brreg data');
        console.log('***********************************************************************');
        console.log('Operasjonen tok: ', (t1 - t0) / 1000, ' sekunder å gjennomføre');

    });

    /**
     *
     * @param {Brreg} value
     * @returns {Veidemannctl}
     */
    function transform(value) {
        return {
            entityName: getEntityName(value.navn.toLowerCase()),
            uri: getUri(value.hjemmeside),
            entityLabel: getEntityLabel(value),
            seedLabel: getSeedLabel(),
            entityDescription: value.organisasjonsform ? value.organisasjonsform.beskrivelse : null,
            seedDescription: ''
        }
    }


    function getEntityName(nameString) {
        let entityName = nameString.charAt(0).toUpperCase() + nameString.slice(1);
        const asRegExp = /\sas$|\sas\s/gm;

        const match = asRegExp.exec(entityName);
        if (match) {
            entityName = entityName.replace(asRegExp, ' AS');
        }
        return entityName;
    }


    /**
     *
     * @param {Brreg} brreg
     * @returns {Array}
     */
    function getEntityLabel(brreg) {
        let labels = [];

        labels.push({
            "key": "source",
            "value": "brreg"
        });

        if (brreg.naeringskode1 !== undefined) {
            labels.push({
                "key": "næringskode1",
                "value": brreg.naeringskode1.kode
            });
        }

        if (brreg.naeringskode2 !== undefined) {
            labels.push({
                "key": "næringskode2",
                "value": brreg.naeringskode2.kode
            });
        }

        if (brreg.organisasjonsnummer !== undefined) {
            labels.push({
                "key": "organisasjonsnummer",
                "value": brreg.organisasjonsnummer
            });
        }

        if (brreg.institusjonellSektorkode !== undefined) {
            labels.push({
                "key": "sektorkode",
                "value": brreg.institusjonellSektorkode.kode
            });
        }

        if (brreg.maalform !== undefined) {
            labels.push({
                "key": "målform",
                "value": brreg.maalform
            });
        }

        if (brreg.forretningsadresse !== undefined) {
            labels.push({
                "key": "kommunenummer",
                "value": brreg.forretningsadresse.kommunenummer
            });
        }

        if (brreg.organisasjonsform !== undefined) {
            labels.push({
                "key": "organisasjonsform",
                "value": brreg.organisasjonsform.kode
            });
        }

        return labels;
    }

    function getSeedLabel() {
        return [
            {
                "key": "source",
                "value": "brreg"
            }
        ];
    }

    /**
     *
     * @param {string} urlString
     * @returns {*}
     */
    function getUri(urlString) {
        const httpProtocol = 'http://';

        let parsedUrl = {};
        try {
            parsedUrl = parse(urlString);
        } catch (err) {
            console.error(err);
        }

        if (!parsedUrl.protocol) {
            try {
                const seedUrl = parse(httpProtocol + urlString);

                if (seedUrl.protocol && seedUrl.slashes && seedUrl.host && seedUrl.href) {
                    return seedUrl.href;
                } else {
                    errorStream.write(urlString + ': ' + JSON.stringify(seedUrl) + '\n');
                }
            } catch (err) {
                console.error(err);
            }
        } else {
            if (parsedUrl.protocol && parsedUrl.slashes && parsedUrl.host && parsedUrl.href) {
                return parsedUrl.href;
            } else {
                errorStream.write(JSON.stringify(parsedUrl) + '\n');
            }
        }
    }
}

module.exports = {createImportList};
