/**
 * Endatabas client for the HTTP API
 */
declare class Endb {
    /**
     * Create an Endb object (Endatabas client for the HTTP API)
     * @param {string} [url="http://localhost:3803/sql"] - HTTP URL to the Endatabas /sql API
     * @param {Object} [options] - HTTP options
     * @param {string} [options.accept="application/ld+json"] - Accept Header content type
     * @param {string} [options.username] - username for HTTP Basic Auth
     * @param {string} [options.password] - password for HTTP Basic Auth
     */
    constructor(url?: string, options?: {
        accept?: string;
        username?: string;
        password?: string;
    });

    /**
     * Execute a SQL statement over HTTP
     * @param {string}  q - SQL query
     * @param {string|Object} [p] - Positional parameters, named parameters, or an array of either
     * @param {string} [m] - Many parameters flag
     * @param {string} [accept] - Accept Header content type
     * @returns {Promise<any[]>} Array of documents
     */
    sql(q: string, p?: any[] | object, m?: boolean, accept?: string): Promise<any[]>;

    /**
     * WARNING: This is an internal signature, used by Template Literal syntax.
     * @param {TemplateStringsArray} strings - Chunked strings, passed in by Template Literal
     * @param {...any[]} values - Positional SQL parameters, passed in by Template Literal
     * @returns {Promise<Array>} Array of documents
     */
    sql(strings: TemplateStringsArray, ...values: any[]): Promise<any[]>;
}

/**
 * Endatabas client for the WebSocket API
 */
declare class EndbWebSocket {
    /**
     * Create an EndbWebSocket object (Endatabas client for the WebSocket API)
     * @param {string} [url="ws://localhost:3803/sql"] - WebSocket URL to the Endatabas /sql API
     * @param {Object} [options] - WebSocket options
     * @param {string} [options.ws] - WebSocket implementation
     * @param {string} [options.username] - username for Basic Auth
     * @param {string} [options.password] - password for Basic Auth
     */
    constructor(url?: string, options?: {
        ws?: any;
        username?: string;
        password?: string;
    });

    /**
     * Close the WebSocket connection
     */
    close(): void;

    /**
     * Execute a SQL statement over a WebSocket with LD-JSON
     * @param {string} q - SQL query
     * @param {Array|Object} [p] - Positional parameters, named parameters, or an array of either
     * @param {boolean} [m] - Many parameters flag
     * @returns {Promise<Array>} Array of documents
     */
    sql(q: string, p?: any[] | object, m?: boolean): Promise<any[]>;

    /**
     * WARNING: This is an internal signature, used by Template Literal syntax.
     * @param {TemplateStringsArray} strings - Chunked strings, passed in by Template Literal
     * @param {...any[]} values - Positional SQL parameters, passed in by Template Literal
     * @returns {Promise<Array>} Array of documents
     */
    sql(strings: TemplateStringsArray, ...values: any[]): Promise<any[]>;
}

export { Endb, EndbWebSocket };
