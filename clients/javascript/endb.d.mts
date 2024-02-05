declare class Endb {
    constructor(url?: string, options?: {
        accept?: string;
        username?: string;
        password?: string;
    });
    sql(q: string, p?: any[] | object, m?: boolean): Promise<any[]>;
    sql(strings: TemplateStringsArray, ...values: any[]): Promise<any[]>;
}
declare class EndbWebSocket {
    constructor(url?: string, options?: {
        ws?: any;
        username?: string;
        password?: string;
    });
    close(): void;
    sql(q: string, p?: any[] | object, m?: boolean): Promise<any[]>;
    sql(strings: TemplateStringsArray, ...values: any[]): Promise<any[]>;
}
export { Endb, EndbWebSocket };
