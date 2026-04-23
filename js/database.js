/**
 * IndexedDB layer + multi-store read/write batches for atomicity.
 */

const DB_NAME = 'SmartStoreDB';
const DB_VERSION = 7;

export class Database {
    constructor() {
        this.db = null;
    }

    async init() {
        return new Promise((resolve, reject) => {
            const request = indexedDB.open(DB_NAME, DB_VERSION);
            request.onerror = () => reject(request.error);
            request.onsuccess = () => {
                this.db = request.result;
                resolve();
            };
            request.onupgradeneeded = (event) => {
                const db = event.target.result;
                const oldVersion = event.oldVersion;

                if (!db.objectStoreNames.contains('products')) {
                    const store = db.createObjectStore('products', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('barcode', 'barcode', { unique: true });
                }

                if (!db.objectStoreNames.contains('invoices')) {
                    const store = db.createObjectStore('invoices', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('created_at', 'created_at');
                    store.createIndex('invoice_number', 'invoice_number', { unique: false });
                } else if (oldVersion < 3) {
                    const tx = event.target.transaction;
                    const inv = tx.objectStore('invoices');
                    if (!inv.indexNames.contains('invoice_number')) {
                        inv.createIndex('invoice_number', 'invoice_number', { unique: false });
                    }
                }

                if (!db.objectStoreNames.contains('expenses')) {
                    const store = db.createObjectStore('expenses', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('created_at', 'created_at');
                }

                if (!db.objectStoreNames.contains('customers')) {
                    db.createObjectStore('customers', { keyPath: 'id', autoIncrement: true });
                }

                if (!db.objectStoreNames.contains('suppliers')) {
                    db.createObjectStore('suppliers', { keyPath: 'id', autoIncrement: true });
                }

                if (!db.objectStoreNames.contains('activity')) {
                    const store = db.createObjectStore('activity', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('created_at', 'created_at');
                }

                if (!db.objectStoreNames.contains('settings')) {
                    db.createObjectStore('settings', { keyPath: 'key' });
                }

                if (!db.objectStoreNames.contains('users')) {
                    const store = db.createObjectStore('users', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('username', 'username', { unique: true });
                }

                if (!db.objectStoreNames.contains('account_entries')) {
                    const store = db.createObjectStore('account_entries', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('party_key', 'party_key');
                    store.createIndex('created_at', 'created_at');
                    store.createIndex('ref_key', 'ref_key');
                    store.createIndex('journal_id', 'journal_id', { unique: false });
                    store.createIndex('account_code', 'account_code', { unique: false });
                } else if (oldVersion < 4 && db.objectStoreNames.contains('account_entries')) {
                    const tx = event.target.transaction;
                    const ae = tx.objectStore('account_entries');
                    if (!ae.indexNames.contains('journal_id')) {
                        ae.createIndex('journal_id', 'journal_id', { unique: false });
                    }
                    if (!ae.indexNames.contains('account_code')) {
                        ae.createIndex('account_code', 'account_code', { unique: false });
                    }
                }

                if (!db.objectStoreNames.contains('vouchers')) {
                    const store = db.createObjectStore('vouchers', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('created_at', 'created_at');
                    store.createIndex('voucher_number', 'voucher_number', { unique: true });
                    store.createIndex('party_key', 'party_key');
                }

                if (!db.objectStoreNames.contains('meta')) {
                    db.createObjectStore('meta', { keyPath: 'key' });
                }

                // جداول المشتريات
                if (!db.objectStoreNames.contains('purchase_orders')) {
                    const store = db.createObjectStore('purchase_orders', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('po_number', 'po_number', { unique: true });
                    store.createIndex('supplier_id', 'supplier_id');
                    store.createIndex('created_at', 'created_at');
                    store.createIndex('status', 'status');
                }

                if (!db.objectStoreNames.contains('purchase_invoices')) {
                    const store = db.createObjectStore('purchase_invoices', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('pi_number', 'pi_number', { unique: true });
                    store.createIndex('supplier_id', 'supplier_id');
                    store.createIndex('created_at', 'created_at');
                    store.createIndex('po_id', 'po_id');
                    store.createIndex('status', 'status');
                }

                if (!db.objectStoreNames.contains('purchase_returns')) {
                    const store = db.createObjectStore('purchase_returns', { keyPath: 'id', autoIncrement: true });
                    store.createIndex('pr_number', 'pr_number', { unique: true });
                    store.createIndex('supplier_id', 'supplier_id');
                    store.createIndex('pi_id', 'pi_id');
                    store.createIndex('created_at', 'created_at');
                }
            };
        });
    }

    /**
     * Atomic multi-store write (synchronous ops only — IDB transaction lifecycle).
     * @param {{ type: 'put'|'add', store: string, value: object }[]} ops
     */
    async executeWrites(ops) {
        if (!ops.length) return;
        const storeNames = [...new Set(ops.map((o) => o.store))];
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(storeNames, 'readwrite');
            tx.oncomplete = () => resolve();
            tx.onerror = () => reject(tx.error);
            tx.onabort = () => reject(tx.error || new Error('aborted'));
            try {
                for (const op of ops) {
                    const s = tx.objectStore(op.store);
                    if (op.type === 'put') {
                        op.value.updated_at = new Date().toISOString();
                        s.put(op.value);
                    } else if (op.type === 'add') {
                        op.value.created_at = op.value.created_at || new Date().toISOString();
                        s.add(op.value);
                    }
                }
            } catch (e) {
                reject(e);
            }
        });
    }

    async getAll(storeName) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(storeName, 'readonly');
            const request = tx.objectStore(storeName).getAll();
            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve(request.result);
        });
    }

    async get(storeName, id) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(storeName, 'readonly');
            const request = tx.objectStore(storeName).get(id);
            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve(request.result);
        });
    }

    async add(storeName, data) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(storeName, 'readwrite');
            const store = tx.objectStore(storeName);
            data.created_at = data.created_at || new Date().toISOString();
            const request = store.add(data);
            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve(request.result);
        });
    }

    async put(storeName, data) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(storeName, 'readwrite');
            const store = tx.objectStore(storeName);
            data.updated_at = new Date().toISOString();
            const request = store.put(data);
            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve(request.result);
        });
    }

    async delete(storeName, id) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(storeName, 'readwrite');
            const request = tx.objectStore(storeName).delete(id);
            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve();
        });
    }

    async getByIndex(storeName, indexName, value) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(storeName, 'readonly');
            const request = tx.objectStore(storeName).index(indexName).get(value);
            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve(request.result);
        });
    }

    async getAllByIndex(storeName, indexName, value) {
        return new Promise((resolve, reject) => {
            const tx = this.db.transaction(storeName, 'readonly');
            const request = tx.objectStore(storeName).index(indexName).getAll(value);
            request.onerror = () => reject(request.error);
            request.onsuccess = () => resolve(request.result);
        });
    }

    async getMeta(key) {
        const row = await this.get('meta', key);
        return row ? row.value : null;
    }

    async setMeta(key, value) {
        await this.put('meta', { key, value });
    }
}
