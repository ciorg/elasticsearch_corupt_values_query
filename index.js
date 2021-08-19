'use strict';

const bunyan = require('bunyan');
const http = require('http');

class EsQueries {
    constructor() {
        this.docs_queried = 0;
        this.max = 100000;
        this.min = 50000;
        this.cluster_http = 'URL_TO_ES_CLUSTER';

        // list of text fields in the docs so the checker can skip them
        this.ignore_fields = [];
        this.index = 'INDEX_NAME';
        this.total_corrupt = 0;

        this.logger = bunyan.createLogger({name: 'value-check'})
    }

    async main() {
        let n = 1;
        while (true) {
            const results = await this.httpRequest();
            this.checkData(results);

            this.docs_queried += results.length;
            this.logger.info(`${n} - results: ${results.length.toLocaleString()}, total: ${this.docs_queried.toLocaleString()}, corrupt: ${this.total_corrupt}`);
            n++;
        }
    }
    
    async httpRequest() {
        const size = this._getSize();

        try {
            const result = await this._makeHttpRequest(`http://${this.cluster_http}/${this.index}/_search?size=${size}`);
            return result.hits.hits.map((doc) => doc._source);
        } catch (e) {
            console.log(e);
        }
    }
 
    async _makeHttpRequest(url) {
        return new Promise((resolve, reject) => {
            http.get(url, (res) => {
                const { statusCode } = res;
                let error;

                if (statusCode !== 200) {
                    this.logger.error(statusCode);
                    reject(`Request Failed Status Code: ${statusCode}`);
                }

                if (error) {
                    res.resume();
                    this.logger.error(error.message);
                    reject(error.message);
                }

                res.setEncoding('utf8');

                let rawData = '';
                res.on('data', (chunk) => { rawData += chunk; });
                res.on('end', () => {
                    try {
                        const parsedData = JSON.parse(rawData);
                        return resolve(parsedData);
                    } catch (e) {
                        this.logger.error(e.message);
                        reject(e.message);
                    }
                });
            }).on('error', (e) => {
                reject(`Got error: ${e.message}`);
            });
        });
    }

    checkData(results) {
        for (let i = 0; i < results.length; i++) {
            const corrupt = this._searchRecord(results[i]);

            if (corrupt) {
                this.logger.info(`query size: ${results.length}, position: ${i}`);
            }
        }
    }

    _searchRecord(record) {
        for (const [k, v] of Object.entries(record)) {
            if(this.ignore_fields.includes(k)) continue;

            if (String(v).includes('e') || String(v).includes('E')) {
                this.total_corrupt++;
                this.logger.info('found one: ', `key: ${k}, value: ${v}`, 'record: ', record);
                return true;
            }
        }

        return false;
    }

    _getSize() {
        return Math.round(Math.random() * (this.max - this.min)) + this.min;
    }
};

const query = new EsQueries();

query.main();
