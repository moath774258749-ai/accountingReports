import { CONFIG, ROLES, PERMISSIONS, SESSION_TTL_MS, LOCK_PERIOD_DAYS, BACKUP_REMINDER_DAYS } from './constants.js';
import {
    convertStock,
    getUnitMultiplier,
    getUnitName,
    lineCost,
    lineRevenue
} from './units.js';
import {
    hashPassword,
    verifyPassword,
    createSessionPayload,
    readSession,
    writeSession,
    clearSession
} from './cryptoAuth.js';
import { Database } from './database.js';
import {
    prepareCompleteSale,
    computeCartTotals,
    getPriceByUnit,
    clampDiscount
} from './invoiceOps.js';
import {
    newJournalId,
    buildExpenseJournal,
    buildReceiptVoucherJournal,
    buildSupplierPaymentJournal,
    entryPartyRunningDelta,
    buildReversalJournal
} from './journalService.js';
import {
    filterEntriesByDate,
    filterEntriesAsOfTo,
    buildTrialBalance,
    buildProfitAndLoss,
    buildGeneralLedger,
    buildSimpleBalanceSheet,
    ledgerAccountOptions,
    partySubledgerTotalsMap,
    sumPartySubledger
} from './accountingReports.js';
import { AC } from './chartOfAccounts.js';

function escapeHtml(s) {
    if (!s) return '';
    return String(s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

export class SmartStoreApp {
    constructor() {
        this.db = new Database();
        this.currentUser = null;
        this.currentScreen = 'dashboard';
        this.cart = [];
        this.selectedUnit = 'piece';
        this.invoiceType = 'sale';
        this.isDarkMode = false;
        this.productCache = null;
        this.cacheTimestamp = 0;
        this.CACHE_DURATION = 30000;
        this.searchTimeout = null;
        this.invSearchTimeout = null;
        this.cartTotal = 0;
        this.cartCost = 0;
        this.cartSubtotal = 0;
        this.accountingTab = 'customers';
        this.lastStatement = null;
    }

    async init() {
        try {
            await this.db.init();
            await this.seedDataIfNeeded();
            await this.migrateLocalSettingsToDb();
            this.loadSettings();
            this.bindEvents();
            this.bindDelegatedUi();
            this.checkAuth();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showToast('خطأ في تهيئة النظام', 'error');
        }
    }

    async migrateLocalSettingsToDb() {
        const keys = ['store_name', 'store_phone', 'invoice_notes', 'opening_balance', 'dark_mode'];
        for (const key of keys) {
            const v = localStorage.getItem(key);
            if (v !== null) {
                const existing = await this.db.get('settings', key);
                if (!existing) await this.db.put('settings', { key, value: v });
            }
        }
    }

    async getSetting(key, fallback = '') {
        const row = await this.db.get('settings', key);
        if (row && row.value != null) return row.value;
        return localStorage.getItem(key) ?? fallback;
    }

    async setSetting(key, value) {
        await this.db.put('settings', { key, value: String(value) });
        localStorage.setItem(key, String(value));
    }

    async seedDataIfNeeded() {
        const products = await this.db.getAll('products');
        if (products.length === 0) {
            const seedProducts = [
                { name: 'كمران', barcode: 'KM001', cost_price: 300, retail_price: 350, wholesale_price: 3200, box_price: 145000, stock: 500, min_stock: 50, category: 'سجائر' },
                { name: 'لوكي', barcode: 'LK001', cost_price: 250, retail_price: 300, wholesale_price: 2700, box_price: 120000, stock: 300, min_stock: 30, category: 'سجائر' },
                { name: 'شملان', barcode: 'SH001', cost_price: 280, retail_price: 330, wholesale_price: 3000, box_price: 135000, stock: 400, min_stock: 40, category: 'سجائر' },
                { name: 'مارلبورو', barcode: 'ML001', cost_price: 400, retail_price: 500, wholesale_price: 4500, box_price: 200000, stock: 200, min_stock: 20, category: 'سجائر' },
                { name: 'وينستون', barcode: 'WN001', cost_price: 350, retail_price: 420, wholesale_price: 3800, box_price: 170000, stock: 250, min_stock: 25, category: 'سجائر' },
                { name: 'امريكان', barcode: 'AM001', cost_price: 320, retail_price: 380, wholesale_price: 3400, box_price: 155000, stock: 350, min_stock: 35, category: 'سجائر' },
                { name: 'دافيد', barcode: 'DV001', cost_price: 450, retail_price: 550, wholesale_price: 5000, box_price: 230000, stock: 150, min_stock: 15, category: 'سجائر' },
                { name: 'بك', barcode: 'BK001', cost_price: 200, retail_price: 250, wholesale_price: 2200, box_price: 100000, stock: 600, min_stock: 60, category: 'سجائر' },
                { name: 'غول', barcode: 'GL001', cost_price: 380, retail_price: 450, wholesale_price: 4200, box_price: 190000, stock: 220, min_stock: 22, category: 'سجائر' },
                { name: 'كامبل', barcode: 'CB001', cost_price: 290, retail_price: 340, wholesale_price: 3100, box_price: 140000, stock: 450, min_stock: 45, category: 'سجائر' }
            ];
            for (const product of seedProducts) {
                await this.db.add('products', product);
            }
            await this.db.put('settings', { key: 'opening_balance', value: '0' });
            await this.db.put('settings', { key: 'current_balance', value: '0' });
        }

        const users = await this.db.getAll('users');
        if (users.length === 0) {
            const defaultUsers = [
                { username: 'admin', password: await hashPassword('admin123'), role: 'admin', name: 'مدير', active: true },
                { username: 'manager', password: await hashPassword('manager123'), role: 'manager', name: 'مدير عام', active: true },
                { username: 'cashier', password: await hashPassword('cashier123'), role: 'cashier', name: 'كاشير', active: true },
                { username: 'developer', password: await hashPassword('dev123'), role: 'developer', name: 'مطور', active: true }
            ];
            for (const user of defaultUsers) {
                await this.db.add('users', user);
            }
        }
    }

    async loadSettings() {
        try {
            const storeName = await this.getSetting('store_name', 'متجري');
            const phone = await this.getSetting('store_phone', '');
            const notes = await this.getSetting('invoice_notes', '');
            const openingBalance = parseFloat(await this.getSetting('opening_balance', '0')) || 0;
            const darkMode = (await this.getSetting('dark_mode', 'false')) === 'true';

            const el = (id) => document.getElementById(id);
            if (el('store-name-display')) el('store-name-display').textContent = storeName;
            if (el('setting-store-name')) el('setting-store-name').value = storeName;
            if (el('setting-phone')) el('setting-phone').value = phone;
            if (el('setting-notes')) el('setting-notes').value = notes;
            if (el('setting-opening-balance')) el('setting-opening-balance').value = openingBalance;
            if (darkMode) document.body.classList.add('dark');
        } catch (e) {
            console.error(e);
        }
    }

    checkAuth() {
        console.log('[checkAuth] Checking authentication state...');
        const legacy = sessionStorage.getItem('current_user');
        if (legacy && !sessionStorage.getItem('current_session')) {
            try {
                const u = JSON.parse(legacy);
                writeSession(createSessionPayload(u));
                sessionStorage.removeItem('current_user');
                console.log('[checkAuth] Migrated legacy session');
            } catch { /* ignore */ }
        }
        const user = readSession(SESSION_TTL_MS);
        console.log('[checkAuth] Session user:', user);
        if (user) {
            this.currentUser = user;
            this.showApp();
            this.logActivity('تسجيل دخول', `دخول المستخدم ${this.currentUser.name}`);
        } else {
            console.log('[checkAuth] No valid session, showing login screen');
        }
    }

    async getProducts(forceRefresh = false) {
        const now = Date.now();
        if (!forceRefresh && this.productCache && (now - this.cacheTimestamp) < this.CACHE_DURATION) {
            return this.productCache;
        }
        this.productCache = await this.db.getAll('products');
        this.cacheTimestamp = now;
        return this.productCache;
    }

    invalidateCache() {
        this.productCache = null;
        this.cacheTimestamp = 0;
    }

    isRecordLocked(createdAt) {
        if (!createdAt) return false;
        const created = new Date(createdAt).getTime();
        const now = Date.now();
        const lockMs = LOCK_PERIOD_DAYS * 24 * 60 * 60 * 1000;
        return (now - created) > lockMs;
    }

    async canDeleteRecord(storeName, id) {
        if (storeName === 'account_entries') return false;
        const record = await this.db.get(storeName, id);
        if (!record) return true;
        if (['invoices', 'expenses', 'vouchers', 'account_entries'].includes(storeName)) {
            return !this.isRecordLocked(record.created_at);
        }
        return true;
    }

    hasPermission(permission) {
        if (!this.currentUser) return false;
        const userPermissions = PERMISSIONS[this.currentUser.role] || [];
        if (userPermissions.includes('view_all')) return true;
        return userPermissions.includes(permission);
    }

    canAccess(screen) {
        if (!this.currentUser) return false;
        const role = this.currentUser.role;
        if (role === ROLES.ADMIN) return true;
        if (role === ROLES.DEVELOPER) return true;
        if (role === ROLES.MANAGER) {
            return ['dashboard', 'pos', 'inventory', 'customers', 'expenses', 'accounting', 'activity'].includes(screen);
        }
        if (role === ROLES.CASHIER) {
            return screen === 'pos' || screen === 'accounting';
        }
        return false;
    }

    applyNavVisibility() {
        const role = this.currentUser?.role;
        document.querySelectorAll('.nav-link[data-screen]').forEach((link) => {
            const s = link.dataset.screen;
            let ok = true;
            if (role === ROLES.MANAGER) {
                ok = !['reports', 'settings'].includes(s);
            }
            if (role === ROLES.CASHIER) {
                ok = s === 'pos' || s === 'accounting';
            }
            if (role === ROLES.DEVELOPER) {
                ok = true;
            }
            link.closest('.nav-item').style.display = ok ? '' : 'none';
        });
    }

    bindEvents() {
        document.getElementById('login-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.handleLogin();
        });
        document.getElementById('logout-btn').addEventListener('click', () => this.handleLogout());
        document.querySelectorAll('.nav-link').forEach((link) => {
            link.addEventListener('click', (e) => {
                e.preventDefault();
                const screen = e.currentTarget.dataset.screen;
                if (screen) this.navigateTo(screen);
            });
        });
        document.querySelectorAll('.type-btn').forEach((btn) => {
            btn.addEventListener('click', (e) => this.setInvoiceType(e.currentTarget.dataset.type));
        });
        document.querySelectorAll('.unit-btn').forEach((btn) => {
            btn.addEventListener('click', (e) => this.setSelectedUnit(e.currentTarget.dataset.unit));
        });
        document.getElementById('pos-search').addEventListener('input', (e) => this.searchProducts(e.target.value));
        document.getElementById('inventory-search').addEventListener('input', (e) => this.searchInventory(e.target.value));
        document.getElementById('clear-cart').addEventListener('click', () => this.clearCart());
        document.getElementById('discount-input').addEventListener('input', () => this.updateCartDisplay());
        document.getElementById('paid-now-input')?.addEventListener('input', () => this.updateCartDisplay());
        document.getElementById('payment-method').addEventListener('change', () => this.updateCartDisplay());
        document.getElementById('customer-select')?.addEventListener('change', () => this.onCustomerSelectChange());
        document.getElementById('complete-sale').addEventListener('click', () => this.completeSale());
        document.getElementById('add-product-btn').addEventListener('click', () => this.openProductModal());
        document.getElementById('product-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.saveProduct();
        });
        document.getElementById('close-product-modal').addEventListener('click', () => this.closeProductModal());
        document.getElementById('add-customer-btn').addEventListener('click', () => this.openCustomerModal());
        document.getElementById('customer-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.saveCustomer();
        });
        document.getElementById('close-customer-modal').addEventListener('click', () => this.closeCustomerModal());
        document.getElementById('add-expense-btn').addEventListener('click', () => this.openExpenseModal());
        document.getElementById('expense-form').addEventListener('submit', (e) => {
            e.preventDefault();
            this.saveExpense();
        });
        document.getElementById('close-expense-modal').addEventListener('click', () => this.closeExpenseModal());
        document.getElementById('save-settings').addEventListener('click', () => this.saveSettings());
        document.getElementById('store-logo-input')?.addEventListener('change', (e) => this.handleLogoUpload(e));
        document.getElementById('reset-system-btn')?.addEventListener('click', () => this.resetSystem());
        document.getElementById('add-purchase-invoice-btn')?.addEventListener('click', () => this.openPurchaseModal());
        document.getElementById('add-purchase-return-btn')?.addEventListener('click', () => this.openReturnModal());
        document.getElementById('purchase-payment-method')?.addEventListener('change', (e) => {
            const group = document.getElementById('paid-amount-group');
            if (group) group.style.display = e.target.value === 'cash' ? 'block' : 'none';
        });
        
        // تبويبات المشتريات
        document.querySelectorAll('[data-purchase-tab]').forEach((tab) => {
            tab.addEventListener('click', (e) => {
                const tabName = e.currentTarget.dataset.purchaseTab;
                document.querySelectorAll('[data-purchase-tab]').forEach((t) => t.classList.remove('active'));
                e.currentTarget.classList.add('active');
                
                const invoicesCard = document.querySelector('.card:has(#purchase-invoices-tbody)');
                const returnsCard = document.getElementById('purchase-returns-card');
                
                if (tabName === 'invoices') {
                    if (invoicesCard) invoicesCard.style.display = 'block';
                    if (returnsCard) returnsCard.style.display = 'none';
                    document.getElementById('add-purchase-invoice-btn').style.display = 'inline-block';
                    document.getElementById('add-purchase-return-btn').style.display = 'none';
                } else {
                    if (invoicesCard) invoicesCard.style.display = 'none';
                    if (returnsCard) returnsCard.style.display = 'block';
                    document.getElementById('add-purchase-invoice-btn').style.display = 'none';
                    document.getElementById('add-purchase-return-btn').style.display = 'inline-block';
                }
            });
        });
        document.getElementById('dark-mode-toggle').addEventListener('click', () => this.toggleDarkMode());
        document.querySelectorAll('[data-report]').forEach((tab) => {
            tab.addEventListener('click', (e) => this.showReport(e.currentTarget.dataset.report));
        });
        document.getElementById('close-print-modal').addEventListener('click', () => {
            document.getElementById('print-modal').classList.remove('active');
        });
        document.getElementById('print-invoice').addEventListener('click', () => window.print());
        document.getElementById('menu-toggle').addEventListener('click', () => {
            document.getElementById('sidebar').classList.toggle('open');
        });

        document.getElementById('add-supplier-btn')?.addEventListener('click', () => this.openSupplierModal());
        document.getElementById('supplier-form')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.saveSupplier();
        });
        document.getElementById('close-supplier-modal')?.addEventListener('click', () => this.closeSupplierModal());
        document.getElementById('voucher-form')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.saveVoucher();
        });
        document.getElementById('close-voucher-modal')?.addEventListener('click', () => this.closeVoucherModal());
        document.getElementById('statement-run')?.addEventListener('click', () => this.runAccountStatement());
        document.getElementById('statement-print')?.addEventListener('click', () => this.printStatement());
        document.querySelectorAll('[data-accounting-tab]').forEach((t) => {
            t.addEventListener('click', (e) => this.switchAccountingTab(e.currentTarget.dataset.accountingTab));
        });
        document.getElementById('export-backup-btn')?.addEventListener('click', () => this.exportBackup());
        document.getElementById('import-backup-input')?.addEventListener('change', (e) => this.importBackup(e.target.files[0]));
        document.getElementById('receipt-voucher-btn')?.addEventListener('click', () => this.openVoucherModal('receipt'));
        document.getElementById('payment-voucher-btn')?.addEventListener('click', () => this.openVoucherModal('payment'));
        document.getElementById('stmt-party-type')?.addEventListener('change', () => this.populateStatementPartyOptions());
        document.getElementById('voucher-party-type')?.addEventListener('change', () => this.populateVoucherPartySelect());
        document.getElementById('add-user-btn')?.addEventListener('click', () => this.openUserModal());
        document.getElementById('user-form')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.saveUser();
        });
        document.getElementById('close-user-modal')?.addEventListener('click', () => this.closeUserModal());
        document.getElementById('report-date-from')?.addEventListener('change', () => this.refreshReportFilters());
        document.getElementById('report-date-to')?.addEventListener('change', () => this.refreshReportFilters());
        document.getElementById('report-export-csv')?.addEventListener('click', () => this.exportCurrentReportCsv());
        document.getElementById('reports-screen')?.addEventListener('click', (e) => {
            if (e.target.closest('.report-print-btn')) window.print();
        });
        document.getElementById('reports-screen')?.addEventListener('change', (e) => {
            if (e.target.id === 'ledger-account-filter') {
                this.refreshReportFilters();
            }
        });

        document.addEventListener('keydown', (e) => {
            if (this.currentScreen === 'pos') {
                if (e.key === 'F2') {
                    e.preventDefault();
                    document.getElementById('pos-search').focus();
                } else if (e.key === 'F10') {
                    e.preventDefault();
                    this.completeSale();
                } else if (e.key === 'Escape') {
                    this.clearCart();
                }
            }
        });
    }

    bindDelegatedUi() {
        document.getElementById('products-grid').addEventListener('click', (e) => {
            const card = e.target.closest('.product-card[data-id]');
            if (!card || card.classList.contains('out-of-stock')) return;
            const id = parseInt(card.dataset.id, 10);
            if (id) this.addToCart(id);
        });
        document.getElementById('cart-items').addEventListener('click', (e) => {
            const t = e.target.closest('[data-cart-action]');
            if (!t) return;
            const action = t.dataset.cartAction;
            const idx = parseInt(t.dataset.index, 10);
            if (action === 'dec') this.changeQuantity(idx, -1);
            if (action === 'inc') this.changeQuantity(idx, 1);
            if (action === 'remove') this.removeFromCart(idx);
        });
        document.getElementById('inventory-table')?.addEventListener('click', (e) => {
            const b = e.target.closest('button[data-inv-action]');
            if (!b) return;
            const id = parseInt(b.dataset.id, 10);
            if (b.dataset.invAction === 'edit') this.editProduct(id);
            if (b.dataset.invAction === 'delete') this.deleteProduct(id);
        });
        const custClick = (e) => {
            const b = e.target.closest('button[data-cust-action]');
            if (!b) return;
            const id = parseInt(b.dataset.id, 10);
            if (b.dataset.custAction === 'edit') this.editCustomer(id);
            if (b.dataset.custAction === 'delete') this.deleteCustomer(id);
        };
        document.getElementById('customers-table')?.addEventListener('click', custClick);
        document.getElementById('accounting-customers-tbody')?.addEventListener('click', custClick);
        document.getElementById('suppliers-table')?.addEventListener('click', (e) => {
            const b = e.target.closest('button[data-sup-action]');
            if (!b) return;
            const id = parseInt(b.dataset.id, 10);
            if (b.dataset.supAction === 'edit') this.editSupplier(id);
            if (b.dataset.supAction === 'delete') this.deleteSupplier(id);
        });
        document.getElementById('users-table')?.addEventListener('click', (e) => {
            const b = e.target.closest('button[data-user-action]');
            if (!b) return;
            const id = parseInt(b.dataset.id, 10);
            if (b.dataset.userAction === 'edit') this.editUser(id);
        });
        document.getElementById('vouchers-list')?.addEventListener('click', (e) => {
            const b = e.target.closest('[data-voucher-print]');
            if (!b || !b.dataset.voucherPrint) return;
            try {
                const v = JSON.parse(decodeURIComponent(b.dataset.voucherPrint));
                this.printVoucherHtml(v);
            } catch (err) {
                console.error(err);
            }
        });
    }

    async handleLogin() {
        try {
            console.log('[Login] Starting login process...');
            const username = document.getElementById('username').value;
            const password = document.getElementById('password').value;
            console.log('[Login] Username:', username);
            if (!username || !password) {
                this.showToast('الرجاء إدخال اسم المستخدم وكلمة المرور', 'error');
                return;
            }
            const users = await this.db.getAll('users');
            console.log('[Login] Found users:', users.length);
            const user = users.find((u) => u.username === username && u.active !== false);
            if (!user || !(await verifyPassword(password, user.password))) {
                console.log('[Login] Invalid credentials');
                this.showToast('اسم المستخدم أو كلمة المرور خطأ', 'error');
                return;
            }
            console.log('[Login] User authenticated:', user.username, 'Role:', user.role);
            if (!user.password.startsWith('pbkdf2$')) {
                const newHash = await hashPassword(password);
                await this.db.put('users', { ...user, password: newHash });
            }
            this.currentUser = {
                id: user.id,
                username: user.username,
                role: user.role,
                name: user.name
            };
            const sessionPayload = createSessionPayload(user);
            writeSession(sessionPayload);
            console.log('[Login] Session written to sessionStorage');
            this.showApp();
            this.logActivity('تسجيل دخول', `دخول المستخدم ${user.name}`);
            this.showToast(`مرحباً ${user.name}`, 'success');
            console.log('[Login] Login complete, navigating to dashboard');
        } catch (e) {
            console.error('[Login] Login error:', e);
        }
    }

    handleLogout() {
        clearSession();
        this.currentUser = null;
        document.getElementById('login-screen').style.display = 'flex';
        document.getElementById('app-layout').style.display = 'none';
        this.showToast('تم تسجيل الخروج', 'success');
    }

    showApp() {
        console.log('[showApp] Hiding login screen, showing app layout');
        document.getElementById('login-screen').style.display = 'none';
        document.getElementById('app-layout').style.display = 'block';
        console.log('[showApp] Login screen display:', document.getElementById('login-screen').style.display);
        console.log('[showApp] App layout display:', document.getElementById('app-layout').style.display);
        document.getElementById('user-display').textContent = this.currentUser.name;
        const badge = document.getElementById('mode-badge');
        if (this.currentUser.role === ROLES.CASHIER) {
            badge.style.display = 'block';
            badge.textContent = 'كاشير';
        } else {
            badge.style.display = 'none';
        }
        this.applyNavVisibility();
        this.checkBackupReminder();
        console.log('[showApp] Current user role:', this.currentUser.role);
        if (this.currentUser.role === ROLES.CASHIER) {
            this.navigateTo('pos');
        } else {
            this.navigateTo('dashboard');
        }
    }

    async checkBackupReminder() {
        if (this.currentUser.role !== ROLES.ADMIN && this.currentUser.role !== ROLES.MANAGER) return;
        const lastBackup = await this.db.getMeta('last_backup');
        if (!lastBackup) {
            this.showToast('ننصح بتصدير نسخة احتياطية الآن - Menu الإعدادات', 'warning');
            return;
        }
        const lastDate = new Date(lastBackup).getTime();
        const now = Date.now();
        const diffDays = (now - lastDate) / (24 * 60 * 60 * 1000);
        if (diffDays > BACKUP_REMINDER_DAYS) {
            this.showToast(`ينصح بتصدير نسخة احتياطية - آخر نسخة منذ ${Math.floor(diffDays)} أيام`, 'warning');
        }
    }

    navigateTo(screen) {
        if (!this.canAccess(screen)) {
            this.showToast('ليس لديك صلاحية للوصول لهذه الصفحة', 'error');
            return;
        }
        this.currentScreen = screen;
        document.querySelectorAll('.nav-link').forEach((link) => {
            link.classList.toggle('active', link.dataset.screen === screen);
        });
        document.querySelectorAll('.screen').forEach((s) => s.classList.remove('active'));
        const screenEl = document.getElementById(`${screen}-screen`);
        if (screenEl) screenEl.classList.add('active');

        const titles = {
            dashboard: 'لوحة التحكم',
            pos: 'نقطة البيع',
            inventory: 'إدارة المخزون',
            customers: 'العملاء',
            expenses: 'المصروفات',
            reports: 'التقارير',
            settings: 'الإعدادات',
            activity: 'سجل النشاط',
            accounting: 'الحسابات'
        };
        document.getElementById('page-title').textContent = titles[screen] || screen;

        switch (screen) {
            case 'dashboard': this.loadDashboard(); break;
            case 'pos': this.loadPOSProducts(); break;
            case 'inventory': this.loadInventory(); break;
            case 'customers': this.loadCustomers(); break;
            case 'expenses': this.loadExpenses(); break;
            case 'reports':
                if (!this.hasPermission('view_reports')) {
                    this.showToast('ليس لديك صلاحية للتقارير', 'error');
                    return;
                }
                this.loadReports(); break;
            case 'settings':
                if (!this.hasPermission('manage_settings')) {
                    this.showToast('ليس لديك صلاحية للإعدادات', 'error');
                    return;
                }
                this.loadSettingsScreen(); break;
            case 'activity': this.loadActivity(); break;
            case 'purchasing': this.loadPurchasingScreen(); break;
            case 'accounting': this.loadAccounting(); break;
            default: break;
        }
    }

    async loadDashboard() {
        try {
            const today = new Date().toISOString().split('T')[0];
            const invoices = await this.db.getAll('invoices');
            const expenses = await this.db.getAll('expenses');
            const todayInvoices = invoices.filter((i) => i.created_at.startsWith(today) && i.status === 'completed');
            const todayExpenses = expenses.filter((e) => e.created_at.startsWith(today));
            const todaySales = todayInvoices.reduce((sum, i) => sum + (i.type === 'sale' ? i.total : -i.total), 0);
            const todayExpensesTotal = todayExpenses.reduce((sum, e) => sum + parseFloat(e.amount), 0);
            const income = todayInvoices
                .filter((i) => i.type === 'sale')
                .reduce((sum, i) => sum + (i.total - (i.cost || 0)), 0);
            const openingBalance = parseFloat(await this.getSetting('opening_balance', '0')) || 0;
            const currentBalance = openingBalance + todaySales - todayExpensesTotal;

            document.getElementById('today-sales').textContent = this.formatCurrency(todaySales);
            document.getElementById('today-income').textContent = this.formatCurrency(income);
            document.getElementById('today-expenses').textContent = this.formatCurrency(todayExpensesTotal);
            document.getElementById('current-balance').textContent = this.formatCurrency(currentBalance);

            const recentInvoices = document.getElementById('recent-invoices');
            const recent = todayInvoices.slice(-5).reverse();
            if (recent.length === 0) {
                recentInvoices.innerHTML = '<div class="empty-state"><p>لا توجد فواتير اليوم</p></div>';
            } else {
                recentInvoices.innerHTML = `<table class="table"><thead><tr><th>الرقم</th><th>الأجمالي</th><th>الحالة</th></tr></thead><tbody>${
                    recent.map((i) => `<tr><td>#${escapeHtml(i.invoice_number)}</td><td>${this.formatCurrency(i.total)}</td><td><span class="badge ${i.type === 'sale' ? 'badge-success' : 'badge-danger'}">${i.type === 'sale' ? 'بيع' : 'مرتجع'}</span></td></tr>`).join('')
                }</tbody></table>`;
            }

            const products = await this.getProducts();
            const lowStock = products.filter((p) => p.stock <= p.min_stock);
            const lowStockEl = document.getElementById('low-stock-products');
            const warn = document.getElementById('low-stock-warning');
            if (warn) warn.style.display = lowStock.length > 0 ? 'flex' : 'none';
            if (lowStock.length === 0) {
                lowStockEl.innerHTML = '<div class="empty-state"><p>لا توجد منتجات منخفضة</p></div>';
            } else {
                lowStockEl.innerHTML = `<table class="table"><thead><tr><th>المنتج</th><th>المخزون</th><th>الحد</th></tr></thead><tbody>${
                    lowStock.map((p) => `<tr><td>${escapeHtml(p.name)}</td><td>${p.stock}</td><td>${p.min_stock}</td></tr>`).join('')
                }</tbody></table>`;
            }
        } catch (e) {
            console.error('Dashboard error:', e);
        }
    }

    async onCustomerSelectChange() {
        const sel = document.getElementById('customer-select');
        const id = sel?.value;
        if (!id) return;
        const c = await this.db.get('customers', parseInt(id, 10));
        if (c) document.getElementById('customer-name-input').value = c.name;
    }

    async loadPOSProducts() {
        const products = await this.getProducts();
        this.renderProducts(products);
        this.populateCustomerSelect();
        this.updateCartDisplay();
    }

    populateCustomerSelect() {
        const sel = document.getElementById('customer-select');
        if (!sel) return;
        this.db.getAll('customers').then((list) => {
            const cur = sel.value;
            sel.innerHTML = '<option value="">— بدون / نقدي مباشر —</option>' +
                list.map((c) => `<option value="${c.id}">${escapeHtml(c.name)}</option>`).join('');
            sel.value = cur;
        });
    }

    renderProducts(products) {
        const grid = document.getElementById('products-grid');
        grid.innerHTML = products.map((p) => {
            const stock = this.getDisplayStock(p);
            const outOfStock = p.stock <= 0;
            const price = getPriceByUnit(p, this.selectedUnit);
            return `
                <div class="card product-card ${outOfStock ? 'out-of-stock' : ''}" data-id="${p.id}">
                    <div class="product-name">${escapeHtml(p.name)}</div>
                    <div class="product-price">${this.formatCurrency(price)}</div>
                    <div class="product-stock">${stock}</div>
                </div>`;
        }).join('');
    }

    getDisplayStock(product) {
        const units = convertStock(product.stock);
        return `${units.boxes} كرتون`;
    }

    searchProducts(query) {
        if (this.searchTimeout) clearTimeout(this.searchTimeout);
        if (!query) {
            this.getProducts().then((ps) => this.renderProducts(ps));
            return;
        }
        this.searchTimeout = setTimeout(() => {
            this.getProducts().then((products) => {
                const q = query.toLowerCase();
                const exactBarcode = products.find((p) => p.barcode && p.barcode.toLowerCase() === q);
                if (exactBarcode) {
                    this.addToCart(exactBarcode.id);
                    document.getElementById('pos-search').value = '';
                    this.getProducts().then((ps) => this.renderProducts(ps));
                    return;
                }
                const filtered = products.filter((p) =>
                    p.name.toLowerCase().includes(q) || (p.barcode && p.barcode.toLowerCase().includes(q))
                );
                this.renderProducts(filtered);
            });
        }, 200);
    }

    setInvoiceType(type) {
        this.invoiceType = type;
        document.querySelectorAll('.type-btn').forEach((btn) => {
            btn.classList.toggle('active', btn.dataset.type === type);
        });
    }

    setSelectedUnit(unit) {
        this.selectedUnit = unit;
        document.querySelectorAll('.unit-btn').forEach((btn) => {
            btn.classList.toggle('active', btn.dataset.unit === unit);
        });
        this.getProducts().then((ps) => this.renderProducts(ps));
    }

    async addToCart(productId) {
        const product = await this.db.get('products', productId);
        if (!product) return;
        const existingItem = this.cart.find((item) => item.product_id === productId && item.unit === this.selectedUnit);
        if (existingItem) {
            existingItem.quantity += 1;
        } else {
            this.cart.push({
                product_id: productId,
                product_name: product.name,
                unit: this.selectedUnit,
                price: getPriceByUnit(product, this.selectedUnit),
                cost: product.cost_price,
                quantity: 1
            });
        }
        this.updateCartDisplay();
        this.showToast(`تم إضافة ${product.name}`, 'success');
    }

    updateCartDisplay() {
        const cartItemsEl = document.getElementById('cart-items');
        const discount = clampDiscount(this.cartSubtotalFromCart(), parseFloat(document.getElementById('discount-input').value) || 0);
        document.getElementById('discount-input').value = String(discount);

        let subtotal = 0;
        let totalCost = 0;
        if (this.cart.length === 0) {
            cartItemsEl.innerHTML = '<div class="empty-state"><i class="fas fa-shopping-cart"></i><p>السلة فارغة</p></div>';
        } else {
            cartItemsEl.innerHTML = this.cart.map((item, index) => {
                const itemTotal = lineRevenue(item.price, item.quantity);
                const itemCost = lineCost(item.cost, item.quantity, item.unit);
                subtotal += itemTotal;
                totalCost += itemCost;
                return `
                    <div class="cart-item">
                        <div class="cart-item-info">
                            <div class="cart-item-name">${escapeHtml(item.product_name)}</div>
                            <div class="cart-item-price">${this.formatCurrency(item.price)} × ${item.quantity} ${getUnitName(item.unit)}</div>
                        </div>
                        <div class="cart-item-qty">
                            <button type="button" class="qty-btn" data-cart-action="dec" data-index="${index}">-</button>
                            <span>${item.quantity}</span>
                            <button type="button" class="qty-btn" data-cart-action="inc" data-index="${index}">+</button>
                        </div>
                        <div style="margin-right: 8px;">${this.formatCurrency(itemTotal)}</div>
                        <button type="button" class="qty-btn" data-cart-action="remove" data-index="${index}" style="margin-right: 8px;"><i class="fas fa-times"></i></button>
                    </div>`;
            }).join('');
        }
        const paymentMethod = document.getElementById('payment-method').value;
        const paidInput = document.getElementById('paid-now-input');
        let paid = 0;
        if (paymentMethod === 'cash') {
            paid = subtotal - discount;
            if (paidInput) paidInput.value = String(paid);
        } else {
            paid = Math.min(Math.max(0, parseFloat(paidInput?.value) || 0), Math.max(0, subtotal - discount));
        }
        const grandTotal = Math.max(0, subtotal - discount);
        const due = Math.max(0, grandTotal - paid);
        document.getElementById('cart-total').textContent = this.formatCurrency(subtotal);
        document.getElementById('cart-grand-total').textContent = this.formatCurrency(grandTotal);
        const dueEl = document.getElementById('cart-due-total');
        if (dueEl) dueEl.textContent = this.formatCurrency(due);
        this.cartTotal = grandTotal;
        this.cartSubtotal = subtotal;
        this.cartCost = totalCost;
    }

    cartSubtotalFromCart() {
        return this.cart.reduce((s, i) => s + lineRevenue(i.price, i.quantity), 0);
    }

    changeQuantity(index, delta) {
        this.cart[index].quantity += delta;
        if (this.cart[index].quantity <= 0) this.cart.splice(index, 1);
        this.updateCartDisplay();
    }

    removeFromCart(index) {
        this.cart.splice(index, 1);
        this.updateCartDisplay();
    }

    clearCart() {
        this.cart = [];
        document.getElementById('customer-name-input').value = '';
        const sel = document.getElementById('customer-select');
        if (sel) sel.value = '';
        document.getElementById('discount-input').value = '0';
        const paid = document.getElementById('paid-now-input');
        if (paid) paid.value = '0';
        this.updateCartDisplay();
        this.showToast('تم إلغاء السلة', 'success');
    }

    async generateInvoiceNumber() {
        const seq = (await this.db.getMeta('invoice_seq')) || 1;
        await this.db.setMeta('invoice_seq', seq + 1);
        return `INV-${String(seq).padStart(5, '0')}`;
    }

    async generateCustomerNumber() {
        const seq = (await this.db.getMeta('customer_seq')) || 1;
        await this.db.setMeta('customer_seq', seq + 1);
        return `CUST-${String(seq).padStart(3, '0')}`;
    }

    async completeSale() {
        if (this.cart.length === 0) {
            this.showToast('السلة فارغة', 'warning');
            return;
        }
        const products = await this.getProducts(true);
        const map = new Map(products.map((p) => [p.id, p]));
        const customerSelect = document.getElementById('customer-select');
        const customerId = customerSelect?.value ? parseInt(customerSelect.value, 10) : null;
        let customerRow = null;
        if (customerId) customerRow = await this.db.get('customers', customerId);

        const discount = clampDiscount(this.cartSubtotalFromCart(), parseFloat(document.getElementById('discount-input').value) || 0);
        const paymentMethod = document.getElementById('payment-method').value;
        const paidInput = document.getElementById('paid-now-input')?.value;
        const invoiceNumber = await this.generateInvoiceNumber();
        const totals = computeCartTotals(this.cart);

        const nameInput = document.getElementById('customer-name-input').value;
        const displayCustomerName = (customerRow && customerRow.name) || nameInput;

        let customerGlBalance = null;
        if (customerId) {
            const allEntries = await this.db.getAll('account_entries');
            customerGlBalance = sumPartySubledger(allEntries, `customer:${customerId}`);
        }

        const prep = prepareCompleteSale({
            cart: this.cart,
            productsById: map,
            invoiceType: this.invoiceType,
            invoiceNumber,
            paymentMethod,
            paidAmountInput: paidInput,
            discountRaw: discount,
            customerId,
            customerRow,
            customerGlBalance,
            customerNameText: displayCustomerName,
            currentUserName: this.currentUser.name,
            cartSubtotal: totals.subtotal,
            cartCost: totals.cost
        });

        if (prep.error) {
            this.showToast(prep.error, 'error');
            return;
        }

        try {
            await this.db.executeWrites(prep.ops);
            if (customerId) await this.syncPartyBalanceFromGl('customer', customerId);
            await this.logActivity('فاتورة جديدة', `تم إنشاء فاتورة ${prep.invoice.invoice_number} بقيمة ${this.formatCurrency(prep.invoice.total)}`);
            await this.printInvoiceWindow(prep.invoice);
            this.clearCart();
            this.loadPOSProducts();
            this.invalidateCache();
            this.showToast('تمت العملية بنجاح', 'success');
        } catch (err) {
            console.error(err);
            this.showToast('فشل حفظ العملية', 'error');
        }
    }

    async showInvoice(invoice) {
        const content = document.getElementById('invoice-content');
        const storeName = await this.getSetting('store_name', 'متجري');
        const phone = await this.getSetting('store_phone', '');
        const logo = await this.getSetting('store_logo', '');
        const logoImg = logo ? `<img src="${logo}" style="max-height:60px;max-width:120px;margin-bottom:8px;">` : '';
        const cust = invoice.customer_name ? `<p><strong>العميل:</strong> ${escapeHtml(invoice.customer_name)}</p>` : '';
        const pm = { cash: 'نقدي', credit: 'آجل', mixed: 'مختلط' }[invoice.payment_method] || invoice.payment_method;

        content.innerHTML = `
            <div class="invoice-header">
                ${logoImg}
                ${logo ? '' : '<div class="invoice-logo"><i class="fas fa-store"></i></div>'}
                <h2>${escapeHtml(storeName)}</h2>
                <p>${escapeHtml(phone)}</p>
                <p>فاتورة رقم: #${escapeHtml(invoice.invoice_number)}</p>
                <p>التاريخ: ${new Date().toLocaleString('ar-SA')}</p>
                ${cust}
                <p><strong>طريقة الدفع:</strong> ${pm}</p>
            </div>
            <table class="invoice-items">
                <thead><tr><th>المنتج</th><th>الكمية</th><th>الإجمالي</th></tr></thead>
                <tbody>
                    ${invoice.items.map((item) => {
            const line = lineRevenue(item.price, item.quantity);
            return `<tr><td>${escapeHtml(item.product_name)}</td><td>${item.quantity} ${getUnitName(item.unit)}</td><td>${this.formatCurrency(line)}</td></tr>`;
        }).join('')}
                </tbody>
            </table>
            <div class="invoice-total">
                <div class="summary-row"><span>الأجمالي:</span><span>${this.formatCurrency(invoice.subtotal)}</span></div>
                ${invoice.discount > 0 ? `<div class="summary-row"><span>الخصم:</span><span>-${this.formatCurrency(invoice.discount)}</span></div>` : ''}
                <div class="summary-row"><span>المدفوع:</span><span>${this.formatCurrency(invoice.paid_amount || 0)}</span></div>
                <div class="summary-row"><span>المتبقي:</span><span>${this.formatCurrency(invoice.due_amount || 0)}</span></div>
                <div class="summary-row summary-total"><span>المطلوب:</span><span class="invoice-total-amount">${this.formatCurrency(invoice.total)}</span></div>
            </div>
            <div style="text-align: center; margin-top: 20px; color: var(--gray);"><p>شكراً لتعاملكم معنا</p></div>`;
        document.getElementById('print-modal').classList.add('active');
    }

    async printInvoiceWindow(invoice) {
        const storeName = await this.getSetting('store_name', 'متجري');
        const phone = await this.getSetting('store_phone', '');
        const logo = await this.getSetting('store_logo', '');
        const cust = invoice.customer_name ? `<p><strong>العميل:</strong> ${escapeHtml(invoice.customer_name)}</p>` : '';
        const pm = { cash: 'نقدي', credit: 'آجل', mixed: 'مختلط' }[invoice.payment_method] || invoice.payment_method;
        
        const logoImg = logo ? `<img src="${logo}" style="max-height:50px;max-width:100px;margin-bottom:10px;">` : '';
        
        const html = `<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<title>فاتورة</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: Tahoma, Arial, sans-serif; width: 80mm; margin: 0 auto; padding: 5mm; font-size: 12px; }
.header { text-align: center; margin-bottom: 10px; }
.header img { max-height: 50px; max-width: 100px; }
.header h2 { font-size: 16px; margin: 5px 0; }
.info { text-align: center; margin-bottom: 10px; }
table { width: 100%; border-collapse: collapse; margin: 10px 0; }
th, td { padding: 4px 2px; text-align: right; border-bottom: 1px dotted #ccc; }
th { border-bottom: 1px solid #000; }
.total { margin-top: 10px; border-top: 1px solid #000; padding-top: 10px; }
.total-row { display: flex; justify-content: space-between; padding: 3px 0; }
.total-final { font-weight: bold; font-size: 14px; border-top: 1px solid #000; padding-top: 5px; margin-top: 5px; }
.footer { text-align: center; margin-top: 15px; color: #666; font-size: 10px; }
@media print { body { width: 80mm; margin: 0; padding: 0; } }
</style>
</head>
<body>
<div class="header">
${logoImg}
<h2>${escapeHtml(storeName)}</h2>
<p>${escapeHtml(phone)}</p>
</div>
<div class="info">
<p>فاتورة رقم: #${escapeHtml(invoice.invoice_number)}</p>
<p>التاريخ: ${new Date().toLocaleString('ar-SA')}</p>
${cust}
<p><strong>طريقة الدفع:</strong> ${pm}</p>
</div>
<table>
<thead><tr><th>الصنف</th><th>الكمية</th><th>السعر</th></tr></thead>
<tbody>
${invoice.items.map(item => `<tr><td>${escapeHtml(item.product_name)}</td><td>${item.quantity}</td><td>${this.formatCurrency(lineRevenue(item.price, item.quantity))}</td></tr>`).join('')}
</tbody>
</table>
<div class="total">
<div class="total-row"><span>الأجمالي:</span><span>${this.formatCurrency(invoice.subtotal)}</span></div>
${invoice.discount > 0 ? `<div class="total-row"><span>الخصم:</span><span>-${this.formatCurrency(invoice.discount)}</span></div>` : ''}
<div class="total-row"><span>المدفوع:</span><span>${this.formatCurrency(invoice.paid_amount || 0)}</span></div>
<div class="total-row"><span>المتبقي:</span><span>${this.formatCurrency(invoice.due_amount || 0)}</span></div>
<div class="total-row total-final"><span>المطلوب:</span><span>${this.formatCurrency(invoice.total)}</span></div>
</div>
<div class="footer">
<p>شكراً لتعاملكم معنا</p>
</div>
</body>
</html>`;
        
        const w = window.open('', '_blank');
        w.document.write(html);
        w.document.close();
        w.print();
    }

    async loadInventory() {
        const products = await this.getProducts(true);
        this.renderInventoryTable(products);
    }

    renderInventoryTable(products) {
        const tbody = document.getElementById('inventory-table');
        const showCost = this.hasPermission('view_cost');
        const head = document.getElementById('inventory-thead-row');
        if (head) {
            head.innerHTML = showCost
                ? '<th>المنتج</th><th>الباركود</th><th>المخزون</th><th>التكلفة</th><th>سعر الجملة</th><th>سعر التجزئة</th><th>الإجراءات</th>'
                : '<th>المنتج</th><th>الباركود</th><th>المخزون</th><th>سعر الجملة</th><th>سعر التجزئة</th><th>الإجراءات</th>';
        }
        tbody.innerHTML = products.map((p) => {
            const costCol = showCost ? `<td>${this.formatCurrency(p.cost_price || 0)}</td>` : '';
            const delBtn = this.currentUser.role === 'admin'
                ? `<button type="button" class="btn btn-sm btn-danger" data-inv-action="delete" data-id="${p.id}"><i class="fas fa-trash"></i></button>`
                : '';
            return `<tr>
                <td>${escapeHtml(p.name)}</td>
                <td>${escapeHtml(p.barcode || '-')}</td>
                <td><span class="badge ${p.stock <= p.min_stock ? 'badge-warning' : 'badge-success'}">${p.stock} حبة</span></td>
                ${costCol}
                <td>${this.formatCurrency(p.wholesale_price || 0)}</td>
                <td>${this.formatCurrency(p.retail_price)}</td>
                <td>
                    <button type="button" class="btn btn-sm btn-outline" data-inv-action="edit" data-id="${p.id}"><i class="fas fa-edit"></i></button>
                    ${delBtn}
                </td>
            </tr>`;
        }).join('');
    }

    searchInventory(query) {
        if (this.invSearchTimeout) clearTimeout(this.invSearchTimeout);
        this.invSearchTimeout = setTimeout(() => {
            this.getProducts(true).then((products) => {
                const q = query.trim();
                const filtered = products.filter((p) =>
                    p.name.includes(q) || (p.barcode && p.barcode.includes(q))
                );
                this.renderInventoryTable(filtered);
            });
        }, 200);
    }

    openProductModal(product = null) {
        if (product) {
            document.getElementById('product-modal-title').textContent = 'تعديل منتج';
            document.getElementById('product-id').value = product.id;
            document.getElementById('product-name').value = product.name;
            document.getElementById('product-barcode').value = product.barcode || '';
            document.getElementById('product-cost').value = product.cost_price;
            document.getElementById('product-retail').value = product.retail_price;
            document.getElementById('product-wholesale').value = product.wholesale_price || '';
            document.getElementById('product-box').value = product.box_price || '';
            document.getElementById('product-stock').value = product.stock;
            document.getElementById('product-min-stock').value = product.min_stock;
        } else {
            document.getElementById('product-modal-title').textContent = 'إضافة منتج جديد';
            document.getElementById('product-form').reset();
            document.getElementById('product-id').value = '';
        }
        document.getElementById('product-modal').classList.add('active');
    }

    async editProduct(id) {
        const product = await this.db.get('products', id);
        if (!product) return;
        if (this.isRecordLocked(product.created_at)) {
            this.showToast('لا يمكن تعديل هذا السجل لأنه أقدم من 30 يوم', 'error');
            return;
        }
        this.openProductModal(product);
    }

    async deleteProduct(id) {
        if (!this.hasPermission('delete_records')) return;
        const product = await this.db.get('products', id);
        if (product && product.stock > 0) {
            this.showToast('لا يمكن حذف منتج له مخزون', 'error');
            return;
        }
        if (confirm('هل أنت متأكد من حذف هذا المنتج؟')) {
            await this.db.delete('products', id);
            this.invalidateCache();
            this.logActivity('حذف منتج', `تم حذف منتج #${id}`);
            this.loadInventory();
            this.showToast('تم الحذف بنجاح', 'success');
        }
    }

    async saveProduct() {
        const id = document.getElementById('product-id').value;
        const stockVal = parseInt(document.getElementById('product-stock').value, 10) || 0;
        if (stockVal < 0) {
            this.showToast('المخزون لا يمكن أن يكون سالباً', 'error');
            return;
        }
        const data = {
            name: document.getElementById('product-name').value,
            barcode: document.getElementById('product-barcode').value,
            cost_price: parseFloat(document.getElementById('product-cost').value) || 0,
            retail_price: parseFloat(document.getElementById('product-retail').value) || 0,
            wholesale_price: parseFloat(document.getElementById('product-wholesale').value) || 0,
            box_price: parseFloat(document.getElementById('product-box').value) || 0,
            stock: stockVal,
            min_stock: parseInt(document.getElementById('product-min-stock').value, 10) || 10
        };
        if (id) {
            data.id = parseInt(id, 10);
            await this.db.put('products', data);
            this.invalidateCache();
            this.logActivity('تعديل منتج', `تم تعديل ${data.name}`);
            this.showToast('تم التعديل بنجاح', 'success');
        } else {
            await this.db.add('products', data);
            this.invalidateCache();
            this.logActivity('إضافة منتج', `تم إضافة ${data.name}`);
            this.showToast('تم الإضافة بنجاح', 'success');
        }
        this.closeProductModal();
        this.loadInventory();
    }

    closeProductModal() {
        document.getElementById('product-modal').classList.remove('active');
    }

    populateStatementPartyOptions() {
        Promise.all([this.db.getAll('customers'), this.db.getAll('suppliers')]).then(([c, s]) => {
            const sel = document.getElementById('stmt-party-id');
            if (!sel) return;
            const t = document.getElementById('stmt-party-type')?.value || 'customer';
            const list = t === 'customer' ? c : s;
            sel.innerHTML = list.map((x) => `<option value="${x.id}">${escapeHtml(x.name)}</option>`).join('');
        });
    }

    async loadCustomers(tbodyId = 'customers-table') {
        const customers = await this.db.getAll('customers');
        const entries = await this.db.getAll('account_entries');
        const partyMap = partySubledgerTotalsMap(entries);
        const rowBal = (c) => {
            const k = `customer:${c.id}`;
            return partyMap.has(k) ? partyMap.get(k) : (c.balance || 0);
        };
        const tbody = document.getElementById(tbodyId);
        if (!tbody) return;
        if (customers.length === 0) {
            tbody.innerHTML = '<tr><td colspan="4" class="empty-state">لا يوجد عملاء</td></tr>';
        } else {
            const canDel = this.currentUser.role === 'admin';
            tbody.innerHTML = customers.map((c) => `
                <tr>
                    <td>${escapeHtml(c.name)}</td>
                    <td>${escapeHtml(c.phone || '-')}</td>
                    <td>${this.formatCurrency(rowBal(c))}</td>
                    <td>
                        <button type="button" class="btn btn-sm btn-outline" data-cust-action="edit" data-id="${c.id}"><i class="fas fa-edit"></i></button>
                        ${canDel ? `<button type="button" class="btn btn-sm btn-danger" data-cust-action="delete" data-id="${c.id}"><i class="fas fa-trash"></i></button>` : ''}
                    </td>
                </tr>`).join('');
        }
    }

    openCustomerModal(customer = null) {
        const title = document.querySelector('#customer-modal h3');
        if (customer) {
            if (title) title.textContent = 'تعديل عميل';
            document.getElementById('customer-id').value = customer.id;
            document.getElementById('customer-name').value = customer.name;
            document.getElementById('customer-phone').value = customer.phone || '';
            document.getElementById('customer-credit-limit').value = customer.credit_limit || 0;
        } else {
            if (title) title.textContent = 'إضافة عميل جديد';
            document.getElementById('customer-form').reset();
            document.getElementById('customer-id').value = '';
        }
        document.getElementById('customer-modal').classList.add('active');
    }

    async editCustomer(id) {
        const c = await this.db.get('customers', id);
        if (!c) return;
        if (this.isRecordLocked(c.created_at)) {
            this.showToast('لا يمكن تعديل هذا السجل لأنه أقدم من 30 يوم', 'error');
            return;
        }
        this.openCustomerModal(c);
    }

    async deleteCustomer(id) {
        if (this.currentUser.role !== 'admin') return;
        const canDelete = await this.canDeleteRecord('customers', id);
        if (!canDelete) {
            this.showToast('لا يمكن حذف هذا السجل لأنه أقدم من 30 يوم', 'error');
            return;
        }
        if (confirm('هل أنت متأكد من حذف هذا العميل؟')) {
            await this.db.delete('customers', id);
            this.loadCustomers();
            this.loadCustomers('accounting-customers-tbody');
            this.showToast('تم الحذف بنجاح', 'success');
        }
    }

    async saveCustomer() {
        const id = document.getElementById('customer-id').value;
        const data = {
            name: document.getElementById('customer-name').value,
            phone: document.getElementById('customer-phone').value,
            credit_limit: parseFloat(document.getElementById('customer-credit-limit').value) || 0,
            balance: 0
        };
        if (id) {
            const prev = await this.db.get('customers', parseInt(id, 10));
            data.id = parseInt(id, 10);
            data.balance = prev.balance || 0;
            data.customer_code = prev.customer_code || await this.generateCustomerNumber();
            await this.db.put('customers', data);
            await this.syncPartyBalanceFromGl('customer', data.id);
            this.showToast('تم التعديل بنجاح', 'success');
        } else {
            data.customer_code = await this.generateCustomerNumber();
            const newId = await this.db.add('customers', data);
            await this.syncPartyBalanceFromGl('customer', newId);
            this.showToast('تم الإضافة بنجاح', 'success');
        }
        this.closeCustomerModal();
        this.loadCustomers();
        this.loadCustomers('accounting-customers-tbody');
    }

    closeCustomerModal() {
        document.getElementById('customer-modal').classList.remove('active');
    }

    async loadExpenses() {
        const expenses = await this.db.getAll('expenses');
        const today = new Date().toISOString().split('T')[0];
        const todayExpenses = expenses.filter((e) => e.created_at.startsWith(today));
        const tbody = document.getElementById('expenses-table');
        if (todayExpenses.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty-state">لا توجد مصروفات اليوم</td></tr>';
        } else {
            tbody.innerHTML = todayExpenses.slice().reverse().map((e) => `
                <tr>
                    <td>${new Date(e.created_at).toLocaleTimeString('ar-SA')}</td>
                    <td>${escapeHtml(e.description || '-')}</td>
                    <td>${CONFIG.EXPENSE_CATEGORIES[e.category] || e.category}</td>
                    <td>${this.formatCurrency(e.amount)}</td>
                    <td>${escapeHtml(e.created_by || '-')}</td>
                </tr>`).join('');
        }
    }

    openExpenseModal() {
        document.getElementById('expense-form').reset();
        document.getElementById('expense-modal').classList.add('active');
    }

    async saveExpense() {
        const data = {
            amount: parseFloat(document.getElementById('expense-amount').value),
            category: document.getElementById('expense-category').value,
            description: document.getElementById('expense-description').value,
            created_by: this.currentUser.name
        };
        const now = new Date().toISOString();
        const refId = `exp-${Date.now()}-${Math.random().toString(36).slice(2, 9)}`;
        const refKey = `expense:${refId}`;
        const journalId = newJournalId();
        const expenseRow = { ...data, created_at: now };
        const entries = buildExpenseJournal(journalId, {
            amount: data.amount,
            refKey,
            refId,
            memo: `${data.description || ''} — ${data.category}`,
            now
        });
        const ops = [
            { type: 'add', store: 'expenses', value: expenseRow },
            ...entries.map((row) => ({ type: 'add', store: 'account_entries', value: row }))
        ];
        await this.db.executeWrites(ops);
        this.logActivity('مصروف', `تم إضافة مصروف ${this.formatCurrency(data.amount)}`);
        this.closeExpenseModal();
        this.loadExpenses();
        this.showToast('تم إضافة المصروف بنجاح', 'success');
    }

    closeExpenseModal() {
        document.getElementById('expense-modal').classList.remove('active');
    }

    async loadReports() {
        const from = document.getElementById('report-date-from');
        const to = document.getElementById('report-date-to');
        const t = new Date().toISOString().split('T')[0];
        if (from && !from.value) from.value = t;
        if (to && !to.value) to.value = t;
        this.showReport('daily');
    }

    refreshReportFilters() {
        const active = document.querySelector('[data-report].active');
        if (active) this.showReport(active.dataset.report);
    }

    async showReport(type) {
        document.querySelectorAll('[data-report]').forEach((tab) => tab.classList.toggle('active', tab.dataset.report === type));
        document.getElementById('daily-report').style.display = type === 'daily' ? 'block' : 'none';
        document.getElementById('products-report').style.display = type === 'products' ? 'block' : 'none';
        document.getElementById('profits-report').style.display = type === 'profits' ? 'block' : 'none';
        document.getElementById('trial-report').style.display = type === 'trial' ? 'block' : 'none';
        document.getElementById('ledger-report').style.display = type === 'ledger' ? 'block' : 'none';
        document.getElementById('balances-report').style.display = type === 'balances' ? 'block' : 'none';
        const bsEl = document.getElementById('bs-report');
        if (bsEl) bsEl.style.display = type === 'bs' ? 'block' : 'none';

        const from = document.getElementById('report-date-from')?.value;
        const to = document.getElementById('report-date-to')?.value;
        if (type === 'daily') await this.showDailyReport(from, to);
        else if (type === 'products') await this.showProductsReport(from, to);
        else if (type === 'profits') await this.showProfitLossFromAccounts(from, to);
        else if (type === 'trial') await this.showTrialBalanceReport(from, to);
        else if (type === 'ledger') await this.showGeneralLedgerReport(from, to);
        else if (type === 'balances') await this.showBalancesReport();
        else if (type === 'bs') await this.showBalanceSheetReport();
    }

    _inDateRange(iso, from, to) {
        const d = (iso || '').split('T')[0];
        return (!from || d >= from) && (!to || d <= to);
    }

    lineBasePiecesFromItem(item) {
        if (item.base_pieces != null) return item.base_pieces;
        const mul = item.unit_multiplier != null ? item.unit_multiplier : getUnitMultiplier(item.unit);
        return (item.quantity || 0) * mul;
    }

    async showDailyReport(from, to) {
        const invoices = await this.db.getAll('invoices');
        const expenses = await this.db.getAll('expenses');
        const dayInvoices = invoices.filter((i) =>
            i.status === 'completed' && this._inDateRange(i.created_at, from, to)
        );
        const dayExpenses = expenses.filter((e) => this._inDateRange(e.created_at, from, to));
        const sales = dayInvoices.filter((i) => i.type === 'sale');
        const returns = dayInvoices.filter((i) => i.type === 'return');
        const totalSales = sales.reduce((sum, i) => sum + i.total, 0);
        const totalReturns = returns.reduce((sum, i) => sum + i.total, 0);
        const totalExpenses = dayExpenses.reduce((sum, e) => sum + parseFloat(e.amount), 0);
        const openingBalance = parseFloat(await this.getSetting('opening_balance', '0')) || 0;
        const currentBalance = openingBalance + totalSales - totalReturns - totalExpenses;

        document.getElementById('daily-report').innerHTML = `
            <h3 style="margin-bottom: 20px;">تقرير فترة</h3>
            <div class="grid grid-2">
                <div class="card"><h4>المبيعات</h4><p class="stat-value">${this.formatCurrency(totalSales)}</p><p>${sales.length} عملية</p></div>
                <div class="card"><h4>المرتجعات</h4><p class="stat-value">${this.formatCurrency(totalReturns)}</p><p>${returns.length} عملية</p></div>
                <div class="card"><h4>المصروفات</h4><p class="stat-value">${this.formatCurrency(totalExpenses)}</p><p>${dayExpenses.length} عملية</p></div>
                <div class="card"><h4>صافي الرصيد</h4><p class="stat-value">${this.formatCurrency(currentBalance)}</p></div>
            </div>
            <button type="button" class="btn btn-primary report-print-btn" style="margin-top: 20px;"><i class="fas fa-print"></i> طباعة</button>`;
    }

    async showProductsReport(from, to) {
        const invoices = await this.db.getAll('invoices');
        const productSales = {};
        invoices.forEach((inv) => {
            if (inv.status !== 'completed' || inv.type !== 'sale') return;
            if (!this._inDateRange(inv.created_at, from, to)) return;
            inv.items.forEach((item) => {
                if (!productSales[item.product_id]) {
                    productSales[item.product_id] = { name: item.product_name, pieces: 0, total: 0 };
                }
                productSales[item.product_id].pieces += this.lineBasePiecesFromItem(item);
                productSales[item.product_id].total += lineRevenue(item.price, item.quantity);
            });
        });
        const sorted = Object.values(productSales).sort((a, b) => b.pieces - a.pieces);
        document.getElementById('products-report').innerHTML = `
            <h3 style="margin-bottom: 20px;">تقرير المنتجات (بالحبة)</h3>
            ${sorted.length === 0 ? '<p>لا توجد بيانات</p>' : `<table class="table"><thead><tr><th>المنتج</th><th>الكمية (حبة)</th><th>الإجمالي</th></tr></thead><tbody>${
                sorted.map((p) => `<tr><td>${escapeHtml(p.name)}</td><td>${p.pieces}</td><td>${this.formatCurrency(p.total)}</td></tr>`).join('')
            }</tbody></table>`}`;
    }

    async showProfitLossFromAccounts(from, to) {
        const all = await this.db.getAll('account_entries');
        const entries = filterEntriesByDate(all, from, to, (iso, f, t) => this._inDateRange(iso, f, t));
        const pl = buildProfitAndLoss(entries);
        const profitColor = pl.netProfit >= 0 ? 'var(--success)' : 'var(--danger)';

        document.getElementById('profits-report').innerHTML = `
            <h3 style="margin-bottom: 20px;">قائمة الدخل (من دفتر الحسابات)</h3>
            <p style="margin-bottom: 16px; color: var(--gray); font-size: 14px;">حسابات: SALES، SALES_DIS، COGS، EXP — حسب حركات <code>account_entries</code> في الفترة.</p>
            <div class="grid grid-2">
                <div class="card"><h4>إيراد المبيعات (إجمالي)</h4><p class="stat-value">${this.formatCurrency(pl.salesGross)}</p></div>
                <div class="card"><h4>خصومات المبيعات</h4><p class="stat-value">${this.formatCurrency(pl.salesDiscounts)}</p></div>
                <div class="card"><h4>صافي المبيعات</h4><p class="stat-value">${this.formatCurrency(pl.netSales)}</p></div>
                <div class="card"><h4>تكلفة البضاعة المباعة</h4><p class="stat-value">${this.formatCurrency(pl.cogsExpense)}</p></div>
                <div class="card"><h4>مجمل الربح</h4><p class="stat-value">${this.formatCurrency(pl.grossProfit)}</p></div>
                <div class="card"><h4>المصروفات التشغيلية</h4><p class="stat-value">${this.formatCurrency(pl.expenseTotal)}</p></div>
            </div>
            <div class="card" style="margin-top: 20px;"><h4>صافي الربح</h4><p class="stat-value" style="color: ${profitColor};">${this.formatCurrency(pl.netProfit)}</p></div>
            <button type="button" class="btn btn-primary report-print-btn" style="margin-top: 20px;"><i class="fas fa-print"></i> طباعة</button>`;
    }

    async showTrialBalanceReport(from, to) {
        const all = await this.db.getAll('account_entries');
        const entries = filterEntriesByDate(all, from, to, (iso, f, t) => this._inDateRange(iso, f, t));
        const tb = buildTrialBalance(entries);
        const ok = tb.balanced;
        const badge = ok
            ? '<span class="badge badge-success">متوازن: إجمالي المدين = إجمالي الدائن</span>'
            : '<span class="badge badge-danger">غير متوازن — راجع البيانات</span>';

        document.getElementById('trial-report').innerHTML = `
            <h3 style="margin-bottom: 20px;">ميزان المراجعة</h3>
            <p style="margin-bottom: 12px;">${badge}</p>
            <p style="margin-bottom: 16px; color: var(--gray); font-size: 14px;">الإجمالي: مدين ${this.formatCurrency(tb.totalDebit)} — دائن ${this.formatCurrency(tb.totalCredit)}${tb.excludedLegacyCount ? ` — سجلات بدون ترميز محاسبي مستبعدة: ${tb.excludedLegacyCount}` : ''}</p>
            ${tb.rows.length === 0 ? '<p>لا توجد حركات مرمّزة في الفترة</p>' : `
            <table class="table">
                <thead><tr><th>رمز الحساب</th><th>البيان</th><th>مدين</th><th>دائن</th></tr></thead>
                <tbody>
                    ${tb.rows.map((r) => `<tr><td>${escapeHtml(r.code)}</td><td>${escapeHtml(r.label)}</td><td>${this.formatCurrency(r.debit)}</td><td>${this.formatCurrency(r.credit)}</td></tr>`).join('')}
                    <tr style="font-weight:700;background:var(--light);"><td colspan="2">الإجمالي</td><td>${this.formatCurrency(tb.totalDebit)}</td><td>${this.formatCurrency(tb.totalCredit)}</td></tr>
                </tbody>
            </table>`}
            <button type="button" class="btn btn-primary report-print-btn" style="margin-top: 20px;"><i class="fas fa-print"></i> طباعة</button>`;
    }

    async showGeneralLedgerReport(from, to) {
        const all = await this.db.getAll('account_entries');
        const entries = filterEntriesByDate(all, from, to, (iso, f, t) => this._inDateRange(iso, f, t));
        const opts = ledgerAccountOptions();
        const selEl = document.getElementById('ledger-account-filter');
        const prev = selEl?.value;
        let accountCode = opts.some((o) => o.code === prev) ? prev : (opts[0]?.code || AC.CASH);
        if (!opts.some((o) => o.code === accountCode)) accountCode = AC.CASH;

        const lines = buildGeneralLedger(entries, accountCode);
        const optHtml = opts.map((o) => `<option value="${escapeHtml(o.code)}" ${o.code === accountCode ? 'selected' : ''}>${escapeHtml(o.label)} (${escapeHtml(o.code)})</option>`).join('');

        document.getElementById('ledger-report').innerHTML = `
            <h3 style="margin-bottom: 20px;">دفتر الأستاذ العام</h3>
            <div class="form-group" style="max-width: 320px; margin-bottom: 16px;">
                <label class="form-label">الحساب</label>
                <select id="ledger-account-filter">${optHtml}</select>
            </div>
            ${lines.length === 0 ? '<p>لا حركات لهذا الحساب في الفترة</p>' : `
            <table class="table">
                <thead><tr><th>التاريخ</th><th>البيان</th><th>مرجع</th><th>مدين</th><th>دائن</th><th>الرصيد الجاري</th></tr></thead>
                <tbody>
                    ${lines.map((ln) => `<tr>
                        <td>${escapeHtml(String(ln.created_at || ''))}</td>
                        <td>${escapeHtml(ln.memo)}</td>
                        <td>${escapeHtml(ln.ref_key)}</td>
                        <td>${this.formatCurrency(ln.debit)}</td>
                        <td>${this.formatCurrency(ln.credit)}</td>
                        <td>${this.formatCurrency(ln.running)}</td>
                    </tr>`).join('')}
                </tbody>
            </table>`}
            <button type="button" class="btn btn-primary report-print-btn" style="margin-top: 20px;"><i class="fas fa-print"></i> طباعة</button>`;
    }

    async showBalancesReport() {
        const customers = await this.db.getAll('customers');
        const suppliers = await this.db.getAll('suppliers');
        const entries = await this.db.getAll('account_entries');
        const partyMap = partySubledgerTotalsMap(entries);
        const custBal = (c) => {
            const k = `customer:${c.id}`;
            return partyMap.has(k) ? partyMap.get(k) : (c.balance || 0);
        };
        const supBal = (s) => {
            const k = `supplier:${s.id}`;
            return partyMap.has(k) ? partyMap.get(k) : (s.balance || 0);
        };
        document.getElementById('balances-report').innerHTML = `
            <h3 style="margin-bottom: 16px;">أرصدة العملاء</h3>
            <p style="margin-bottom: 12px; color: var(--gray); font-size: 14px;">الرصيد من حركات <code>account_entries</code> (ذمم العميل) عند توفرها؛ وإلا الرصيد المخزّن.</p>
            <table class="table"><thead><tr><th>الاسم</th><th>الرصيد</th></tr></thead><tbody>${
                customers.map((c) => `<tr><td>${escapeHtml(c.name)}</td><td>${this.formatCurrency(custBal(c))}</td></tr>`).join('') || '<tr><td colspan="2">لا بيانات</td></tr>'
            }</tbody></table>
            <h3 style="margin: 24px 0 16px;">أرصدة الموردين</h3>
            <table class="table"><thead><tr><th>الاسم</th><th>المستحق له</th></tr></thead><tbody>${
                suppliers.map((s) => `<tr><td>${escapeHtml(s.name)}</td><td>${this.formatCurrency(supBal(s))}</td></tr>`).join('') || '<tr><td colspan="2">لا بيانات</td></tr>'
            }</tbody></table>`;
    }

    /** Point in time: coded GL lines through «إلى تاريخ» (inclusive). */
    async showBalanceSheetReport() {
        const to = document.getElementById('report-date-to')?.value || '';
        const all = await this.db.getAll('account_entries');
        const asOf = filterEntriesAsOfTo(all, to);
        const bs = buildSimpleBalanceSheet(asOf);
        const asOfLabel = to ? `حتى ${escapeHtml(to)} (شامل)` : 'كل الفترات (بدون تقييد تاريخ نهاية)';
        document.getElementById('bs-report').innerHTML = `
            <h3 style="margin-bottom: 12px;">الميزانية العمومية (مبسطة)</h3>
            <p style="margin-bottom: 20px; color: var(--gray); font-size: 14px;">${asOfLabel} — أصول: نقد + مخزون + عملاء | خصوم: موردون | حقوق الملكية = الأصول − الخصوم. حسب سطور GL المرمّزة فقط.</p>
            <h4 style="margin-bottom: 8px;">الأصول</h4>
            <table class="table" style="margin-bottom: 20px;">
                <thead><tr><th>البند</th><th>الرمز</th><th>المبلغ</th></tr></thead>
                <tbody>
                    <tr><td>النقدية</td><td>${AC.CASH}</td><td>${this.formatCurrency(bs.cash)}</td></tr>
                    <tr><td>المخزون</td><td>${AC.INV}</td><td>${this.formatCurrency(bs.inv)}</td></tr>
                    <tr><td>العملاء (ذمم)</td><td>${AC.AR}</td><td>${this.formatCurrency(bs.ar)}</td></tr>
                    <tr style="font-weight:700;background:var(--light);"><td colspan="2">إجمالي الأصول</td><td>${this.formatCurrency(bs.assets)}</td></tr>
                </tbody>
            </table>
            <h4 style="margin-bottom: 8px;">الخصوم</h4>
            <table class="table" style="margin-bottom: 20px;">
                <thead><tr><th>البند</th><th>الرمز</th><th>المبلغ</th></tr></thead>
                <tbody>
                    <tr><td>الموردون (ذمم)</td><td>${AC.AP}</td><td>${this.formatCurrency(bs.ap)}</td></tr>
                    <tr style="font-weight:700;background:var(--light);"><td colspan="2">إجمالي الخصوم</td><td>${this.formatCurrency(bs.liabilities)}</td></tr>
                </tbody>
            </table>
            <div class="card"><h4>حقوق الملكية (محسوبة)</h4><p class="stat-value">${this.formatCurrency(bs.equity)}</p><p style="color: var(--gray); font-size: 14px;">الأصول − الخصوم</p></div>
            <button type="button" class="btn btn-primary report-print-btn" style="margin-top: 20px;"><i class="fas fa-print"></i> طباعة</button>`;
    }

    async exportCurrentReportCsv() {
        if (!this.hasPermission('view_reports')) return;
        const active = document.querySelector('[data-report].active')?.dataset.report || 'daily';
        const from = document.getElementById('report-date-from')?.value || '';
        const to = document.getElementById('report-date-to')?.value || '';
        let rows;
        let filename;
        if (active === 'products') {
            const invoices = await this.db.getAll('invoices');
            const productSales = {};
            invoices.forEach((inv) => {
                if (inv.status !== 'completed' || inv.type !== 'sale' || !this._inDateRange(inv.created_at, from, to)) return;
                inv.items.forEach((item) => {
                    if (!productSales[item.product_id]) {
                        productSales[item.product_id] = { name: item.product_name, pieces: 0, total: 0 };
                    }
                    productSales[item.product_id].pieces += this.lineBasePiecesFromItem(item);
                    productSales[item.product_id].total += lineRevenue(item.price, item.quantity);
                });
            });
            rows = [['المنتج', 'الكمية حبة', 'الإجمالي']];
            Object.values(productSales).forEach((p) => {
                rows.push([p.name, String(p.pieces), String(p.total)]);
            });
            filename = `products-${from || 'all'}.csv`;
        } else if (active === 'trial') {
            const all = await this.db.getAll('account_entries');
            const entries = filterEntriesByDate(all, from, to, (iso, f, t) => this._inDateRange(iso, f, t));
            const tb = buildTrialBalance(entries);
            rows = [['رمز', 'البيان', 'مدين', 'دائن']];
            tb.rows.forEach((r) => rows.push([r.code, r.label, String(r.debit), String(r.credit)]));
            rows.push(['الإجمالي', '', String(tb.totalDebit), String(tb.totalCredit)]);
            filename = `trial-balance-${from || 'all'}.csv`;
        } else if (active === 'profits') {
            const all = await this.db.getAll('account_entries');
            const entries = filterEntriesByDate(all, from, to, (iso, f, t) => this._inDateRange(iso, f, t));
            const pl = buildProfitAndLoss(entries);
            rows = [
                ['البند', 'المبلغ'],
                ['إيراد المبيعات (إجمالي)', String(pl.salesGross)],
                ['خصومات المبيعات', String(pl.salesDiscounts)],
                ['صافي المبيعات', String(pl.netSales)],
                ['تكلفة البضاعة المباعة', String(pl.cogsExpense)],
                ['مجمل الربح', String(pl.grossProfit)],
                ['المصروفات', String(pl.expenseTotal)],
                ['صافي الربح', String(pl.netProfit)]
            ];
            filename = `pl-${from || 'all'}.csv`;
        } else if (active === 'ledger') {
            const all = await this.db.getAll('account_entries');
            const entries = filterEntriesByDate(all, from, to, (iso, f, t) => this._inDateRange(iso, f, t));
            const code = document.getElementById('ledger-account-filter')?.value || AC.CASH;
            const lines = buildGeneralLedger(entries, code);
            rows = [['التاريخ', 'البيان', 'مرجع', 'مدين', 'دائن', 'الرصيد الجاري']];
            lines.forEach((ln) => rows.push([
                String(ln.created_at || ''),
                ln.memo || '',
                ln.ref_key || '',
                String(ln.debit),
                String(ln.credit),
                String(ln.delta),
                String(ln.running)
            ]));
            filename = `ledger-${code}-${from || 'all'}.csv`;
        } else if (active === 'balances') {
            const customers = await this.db.getAll('customers');
            const suppliers = await this.db.getAll('suppliers');
            const entries = await this.db.getAll('account_entries');
            const partyMap = partySubledgerTotalsMap(entries);
            rows = [['نوع', 'الاسم', 'الرصيد']];
            customers.forEach((c) => {
                const k = `customer:${c.id}`;
                const b = partyMap.has(k) ? partyMap.get(k) : (c.balance || 0);
                rows.push(['عميل', c.name, String(b)]);
            });
            suppliers.forEach((s) => {
                const k = `supplier:${s.id}`;
                const b = partyMap.has(k) ? partyMap.get(k) : (s.balance || 0);
                rows.push(['مورد', s.name, String(b)]);
            });
            filename = `balances-${to || 'all'}.csv`;
        } else if (active === 'bs') {
            const to = document.getElementById('report-date-to')?.value || '';
            const all = await this.db.getAll('account_entries');
            const asOf = filterEntriesAsOfTo(all, to);
            const bs = buildSimpleBalanceSheet(asOf);
            rows = [
                ['البند', 'الرمز', 'المبلغ'],
                ['النقدية', AC.CASH, String(bs.cash)],
                ['المخزون', AC.INV, String(bs.inv)],
                ['العملاء', AC.AR, String(bs.ar)],
                ['إجمالي الأصول', '', String(bs.assets)],
                ['الموردون', AC.AP, String(bs.ap)],
                ['إجمالي الخصوم', '', String(bs.liabilities)],
                ['حقوق الملكية', '', String(bs.equity)]
            ];
            filename = `balance-sheet-${to || 'all'}.csv`;
        } else {
            const invoices = await this.db.getAll('invoices');
            rows = [['نوع', 'رقم', 'التاريخ', 'الإجمالي', 'الخصم', 'المدفوع', 'المتبقي']];
            invoices
                .filter((i) => i.status === 'completed' && this._inDateRange(i.created_at, from, to))
                .forEach((i) => {
                    rows.push([
                        i.type,
                        i.invoice_number,
                        i.created_at,
                        String(i.total),
                        String(i.discount || 0),
                        String(i.paid_amount ?? ''),
                        String(i.due_amount ?? '')
                    ]);
                });
            filename = `invoices-${from || 'all'}.csv`;
        }
        const csv = rows.map((r) => r.map((c) => `"${String(c).replace(/"/g, '""')}"`).join(',')).join('\n');
        const blob = new Blob(['\ufeff' + csv], { type: 'text/csv;charset=utf-8;' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = filename;
        a.click();
        URL.revokeObjectURL(a.href);
        this.showToast('تم تصدير CSV', 'success');
    }

    async saveSettings() {
        const storeName = document.getElementById('setting-store-name').value;
        const phone = document.getElementById('setting-phone').value;
        const notes = document.getElementById('setting-notes').value;
        const openingBalance = document.getElementById('setting-opening-balance').value;
        await this.setSetting('store_name', storeName);
        await this.setSetting('store_phone', phone);
        await this.setSetting('invoice_notes', notes);
        await this.setSetting('opening_balance', openingBalance);
        document.getElementById('store-name-display').textContent = storeName;
        await this.loadSettings();
        this.showToast('تم حفظ الإعدادات بنجاح', 'success');
    }

    async resetSystem() {
        if (this.currentUser.role !== 'developer') {
            this.showToast('ليس لديك صلاحية لإعادة تعيين النظام', 'error');
            return;
        }
        if (!confirm('هل أنت متأكد من رغبتك في إعادة تعيين النظام؟ سيتم حذف جميع البيانات!')) {
            return;
        }
        if (!confirm('تحذير: هذا الإجراء لا يمكن التراجع عنه. هل أنت متأكد فعلاً؟')) {
            return;
        }
        try {
            const stores = ['products', 'customers', 'suppliers', 'invoices', 'vouchers', 'expenses', 'account_entries', 'users', 'activity_log', 'meta'];
            for (const store of stores) {
                const items = await this.db.getAll(store);
                for (const item of items) {
                    await this.db.delete(store, item.id || item.key);
                }
            }
            await this.seedDataIfNeeded();
            this.showToast('تم إعادة تعيين النظام بنجاح', 'success');
            setTimeout(() => window.location.reload(), 1500);
        } catch (err) {
            this.showToast('حدث خطأ أثناء إعادة التعيين: ' + err.message, 'error');
        }
    }

    async handleLogoUpload(e) {
        const file = e.target.files?.[0];
        if (!file) return;
        if (!file.type.startsWith('image/')) {
            this.showToast('يجب أن تكون صورة', 'error');
            return;
        }
        const reader = new FileReader();
        reader.onload = async () => {
            await this.setSetting('store_logo', reader.result);
            this.showLogoInHeader();
            this.showToast('تم حفظ الشعار', 'success');
        };
        reader.readAsDataURL(file);
    }

    async showLogoInHeader() {
        const logo = await this.getSetting('store_logo', '');
        const img = document.getElementById('header-logo-img');
        if (img) img.src = logo;
        const icon = document.getElementById('header-logo-icon');
        if (icon) icon.style.display = logo ? 'none' : '';
    }

    async loadSettingsScreen() {
        await this.loadSettings();
        await this.showLogoInHeader();
        const resetBtn = document.getElementById('reset-system-btn');
        if (resetBtn) {
            resetBtn.style.display = this.currentUser.role === 'developer' ? 'inline-flex' : 'none';
        }
        const uc = document.getElementById('users-admin-card');
        const bc = document.getElementById('backup-card');
        if (uc) uc.style.display = this.hasPermission('manage_users') ? 'block' : 'none';
        if (bc) bc.style.display = this.hasPermission('manage_settings') ? 'block' : 'none';
        if (this.hasPermission('manage_users')) await this.loadUsersTable();
    }

    toggleDarkMode() {
        document.body.classList.toggle('dark');
        this.isDarkMode = document.body.classList.contains('dark');
        this.setSetting('dark_mode', String(this.isDarkMode));
    }

    async logActivity(action, details, extra = null) {
        const entry = {
            action,
            details,
            user: this.currentUser?.name || 'النظام',
            timestamp: new Date().toISOString()
        };
        if (extra) entry.extra = extra;
        await this.db.add('activity', entry);
    }

    async loadActivity() {
        const activities = await this.db.getAll('activity');
        const recent = activities.slice().reverse().slice(0, 50);
        document.getElementById('activity-log').innerHTML = recent.map((a) => `
            <div class="activity-item">
                <div><strong>${escapeHtml(a.action)}</strong></div>
                <div>${escapeHtml(a.details || '')}</div>
                <div class="activity-time">${new Date(a.created_at).toLocaleString('ar-SA')} - ${escapeHtml(a.user)}</div>
            </div>`).join('');
    }

    formatCurrency(amount) {
        return new Intl.NumberFormat('ar-YE', { style: 'decimal', minimumFractionDigits: 0 }).format(amount || 0) + ' ريال';
    }

    showToast(message, type = 'success') {
        const container = document.getElementById('toast-container');
        const toast = document.createElement('div');
        toast.className = `toast ${type}`;
        toast.innerHTML = `<i class="fas fa-${type === 'success' ? 'check-circle' : type === 'error' ? 'exclamation-circle' : 'info-circle'}"></i><span>${escapeHtml(message)}</span>`;
        container.appendChild(toast);
        setTimeout(() => {
            toast.style.animation = 'slideIn 0.3s ease reverse';
            setTimeout(() => toast.remove(), 300);
        }, 3000);
    }

    // ——— Accounting ———
    /** Persist party row.balance from sub-ledger sum (single source: account_entries). */
    async syncPartyBalanceFromGl(partyType, partyId) {
        if (partyId == null || Number.isNaN(Number(partyId))) return;
        const id = Number(partyId);
        const key = partyType === 'customer' ? `customer:${id}` : `supplier:${id}`;
        const store = partyType === 'customer' ? 'customers' : 'suppliers';
        const all = await this.db.getAll('account_entries');
        const balance = sumPartySubledger(all, key);
        const row = await this.db.get(store, id);
        if (!row) return;
        await this.db.put(store, { ...row, balance });
    }

    switchAccountingTab(tab) {
        this.accountingTab = tab;
        document.querySelectorAll('[data-accounting-tab]').forEach((b) => {
            b.classList.toggle('active', b.dataset.accountingTab === tab);
        });
        document.querySelectorAll('[data-accounting-pane]').forEach((p) => {
            p.style.display = p.dataset.accountingPane === tab ? 'block' : 'none';
        });
        if (tab === 'customers') this.loadCustomers('accounting-customers-tbody');
        if (tab === 'suppliers') this.loadSuppliers();
        if (tab === 'vouchers') this.loadVouchersList();
        if (tab === 'statement') this.populateStatementPartyOptions();
    }

    async loadAccounting() {
        this.switchAccountingTab(this.accountingTab || 'customers');
    }

    async loadSuppliers() {
        const list = await this.db.getAll('suppliers');
        const entries = await this.db.getAll('account_entries');
        const partyMap = partySubledgerTotalsMap(entries);
        const rowBal = (s) => {
            const k = `supplier:${s.id}`;
            return partyMap.has(k) ? partyMap.get(k) : (s.balance || 0);
        };
        const tbody = document.getElementById('suppliers-table');
        if (!tbody) return;
        if (!list.length) {
            tbody.innerHTML = '<tr><td colspan="4" class="empty-state">لا يوجد موردون</td></tr>';
        } else {
            const canDel = this.currentUser.role === 'admin';
            tbody.innerHTML = list.map((s) => `
                <tr>
                    <td>${escapeHtml(s.name)}</td>
                    <td>${escapeHtml(s.phone || '-')}</td>
                    <td>${this.formatCurrency(rowBal(s))}</td>
                    <td>
                        <button type="button" class="btn btn-sm btn-outline" data-sup-action="edit" data-id="${s.id}"><i class="fas fa-edit"></i></button>
                        ${canDel ? `<button type="button" class="btn btn-sm btn-danger" data-sup-action="delete" data-id="${s.id}"><i class="fas fa-trash"></i></button>` : ''}
                    </td>
                </tr>`).join('');
        }
    }

    openSupplierModal(s = null) {
        const title = document.querySelector('#supplier-modal h3');
        if (s) {
            if (title) title.textContent = 'تعديل مورد';
            document.getElementById('supplier-id').value = s.id;
            document.getElementById('supplier-name').value = s.name;
            document.getElementById('supplier-phone').value = s.phone || '';
        } else {
            if (title) title.textContent = 'إضافة مورد';
            document.getElementById('supplier-form').reset();
            document.getElementById('supplier-id').value = '';
        }
        document.getElementById('supplier-modal').classList.add('active');
    }

    async editSupplier(id) {
        const s = await this.db.get('suppliers', id);
        if (!s) return;
        if (this.isRecordLocked(s.created_at)) {
            this.showToast('لا يمكن تعديل هذا السجل لأنه أقدم من 30 يوم', 'error');
            return;
        }
        this.openSupplierModal(s);
    }

    async deleteSupplier(id) {
        if (this.currentUser.role !== 'admin') return;
        const canDelete = await this.canDeleteRecord('suppliers', id);
        if (!canDelete) {
            this.showToast('لا يمكن حذف هذا السجل لأنه أقدم من 30 يوم', 'error');
            return;
        }
        if (confirm('حذف المورد؟')) {
            await this.db.delete('suppliers', id);
            this.loadSuppliers();
        }
    }

    async saveSupplier() {
        const id = document.getElementById('supplier-id').value;
        const data = {
            name: document.getElementById('supplier-name').value,
            phone: document.getElementById('supplier-phone').value,
            balance: 0
        };
        if (id) {
            const prev = await this.db.get('suppliers', parseInt(id, 10));
            data.id = parseInt(id, 10);
            data.balance = prev.balance || 0;
            await this.db.put('suppliers', data);
            await this.syncPartyBalanceFromGl('supplier', data.id);
        } else {
            const newId = await this.db.add('suppliers', data);
            await this.syncPartyBalanceFromGl('supplier', newId);
        }
        this.closeSupplierModal();
        this.loadSuppliers();
        this.showToast('تم الحفظ', 'success');
    }

    closeSupplierModal() {
        document.getElementById('supplier-modal').classList.remove('active');
    }

    async openVoucherModal(type) {
        document.getElementById('voucher-type').value = type;
        const pt = document.getElementById('voucher-party-type');
        if (pt) pt.value = type === 'receipt' ? 'customer' : 'supplier';
        document.getElementById('voucher-amount').value = '';
        document.getElementById('voucher-memo').value = '';
        await this.populateVoucherPartySelect();
        document.getElementById('voucher-modal').classList.add('active');
    }

    async populateVoucherPartySelect() {
        const partyType = document.getElementById('voucher-party-type')?.value || 'customer';
        const sel = document.getElementById('voucher-party-id');
        if (!sel) return;
        const list = partyType === 'customer' ? await this.db.getAll('customers') : await this.db.getAll('suppliers');
        sel.innerHTML = list.map((x) => `<option value="${x.id}">${escapeHtml(x.name)}</option>`).join('');
    }

    closeVoucherModal() {
        document.getElementById('voucher-modal').classList.remove('active');
    }

    async saveVoucher() {
        const type = document.getElementById('voucher-type').value;
        const partyType = document.getElementById('voucher-party-type').value;
        const partyId = parseInt(document.getElementById('voucher-party-id').value, 10);
        const amount = parseFloat(document.getElementById('voucher-amount').value) || 0;
        const memo = document.getElementById('voucher-memo').value;
        if (!partyId || amount <= 0) {
            this.showToast('أدخل الطرف والمبلغ', 'error');
            return;
        }
        if (type === 'receipt' && partyType !== 'customer') {
            this.showToast('سند القبض للعملاء فقط', 'error');
            return;
        }
        if (type === 'payment' && partyType !== 'supplier') {
            this.showToast('سند الصرف للموردين فقط', 'error');
            return;
        }
        const seq = (await this.db.getMeta('voucher_seq')) || 1;
        const num = `VCH-${String(seq).padStart(5, '0')}`;
        const now = new Date().toISOString();
        const voucher = {
            voucher_number: num,
            voucher_type: type,
            party_type: partyType,
            party_id: partyId,
            amount,
            memo,
            created_by: this.currentUser.name,
            created_at: now
        };

        let partyRow;
        if (partyType === 'customer') partyRow = await this.db.get('customers', partyId);
        else partyRow = await this.db.get('suppliers', partyId);
        if (!partyRow) {
            this.showToast('الطرف غير موجود', 'error');
            return;
        }

        const journalId = newJournalId();
        const glRows =
            type === 'receipt'
                ? buildReceiptVoucherJournal(journalId, {
                    amount,
                    partyId,
                    voucherNumber: num,
                    memo,
                    now
                })
                : buildSupplierPaymentJournal(journalId, {
                    amount,
                    partyId,
                    voucherNumber: num,
                    memo,
                    now
                });

        const ops = [
            { type: 'add', store: 'vouchers', value: voucher },
            { type: 'put', store: 'meta', value: { key: 'voucher_seq', value: seq + 1 } },
            ...glRows.map((row) => ({ type: 'add', store: 'account_entries', value: row }))
        ];

        await this.db.executeWrites(ops);
        if (type === 'receipt') await this.syncPartyBalanceFromGl('customer', partyId);
        else await this.syncPartyBalanceFromGl('supplier', partyId);
        this.logActivity('سند', `${type} ${num}`);
        this.closeVoucherModal();
        this.loadVouchersList();
        this.loadCustomers('accounting-customers-tbody');
        this.loadSuppliers();
        this.populateCustomerSelect();
        this.showToast('تم تسجيل السند', 'success');
    }

    async loadVouchersList() {
        const list = (await this.db.getAll('vouchers')).slice().reverse().slice(0, 100);
        const el = document.getElementById('vouchers-list');
        if (!el) return;
        el.innerHTML = list.length ? `<table class="table"><thead><tr><th>الرقم</th><th>النوع</th><th>المبلغ</th><th>طباعة</th></tr></thead><tbody>${
            list.map((v) => `<tr>
                <td>${escapeHtml(v.voucher_number)}</td>
                <td>${v.voucher_type === 'receipt' ? 'قبض' : 'صرف'}</td>
                <td>${this.formatCurrency(v.amount)}</td>
                <td><button type="button" class="btn btn-sm btn-outline" data-voucher-print="${encodeURIComponent(JSON.stringify(v))}">طباعة</button></td>
            </tr>`).join('')
        }</tbody></table>` : '<p class="empty-state">لا سندات</p>';
    }

    async printVoucherHtml(v) {
        const storeName = await this.getSetting('store_name', 'متجري');
        const phone = await this.getSetting('store_phone', '');
        const logo = await this.getSetting('store_logo', '');
        const logoImg = logo ? `<img src="${logo}" style="max-height:50px;max-width:100px;margin-bottom:10px;">` : '';
        
        const w = window.open('', '_blank');
        const title = v.voucher_type === 'receipt' ? 'سند قبض' : 'سند صرف';
        const html = `<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<title>${title}</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: Tahoma, Arial, sans-serif; width: 80mm; margin: 0 auto; padding: 5mm; font-size: 12px; }
.header { text-align: center; margin-bottom: 15px; }
.header img { max-height: 50px; max-width: 100px; }
.box { border: 2px solid #000; padding: 15px; text-align: center; }
.box h2 { font-size: 16px; margin-bottom: 10px; border-bottom: 1px solid #000; padding-bottom: 10px; }
.row { display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px dotted #ccc; }
.row:last-child { border-bottom: none; }
.footer { text-align: center; margin-top: 15px; color: #666; font-size: 10px; }
@media print { body { width: 80mm; margin: 0; padding: 0; } }
</style>
</head>
<body>
<div class="header">
${logoImg}
<h2>${escapeHtml(storeName)}</h2>
<p>${escapeHtml(phone)}</p>
</div>
<div class="box">
<h2>${title}</h2>
<div class="row"><span>رقم السند:</span><span>${escapeHtml(v.voucher_number)}</span></div>
<div class="row"><span>المبلغ:</span><span>${this.formatCurrency(v.amount)}</span></div>
<div class="row"><span>البيان:</span><span>${escapeHtml(v.memo || '—')}</span></div>
<div class="row"><span>التاريخ:</span><span>${new Date(v.created_at).toLocaleString('ar-SA')}</span></div>
</div>
<div class="footer">
<p>شكراً لتعاملكم معنا</p>
</div>
</body>
</html>`;
        w.document.write(html);
        w.document.close();
        w.print();
    }

    async runAccountStatement() {
        const partyType = document.getElementById('stmt-party-type').value;
        const partyId = parseInt(document.getElementById('stmt-party-id').value, 10);
        const out = document.getElementById('statement-output');
        if (!partyId || Number.isNaN(partyId)) {
            out.innerHTML = '<p>اختر الطرف</p>';
            return;
        }
        const key = `${partyType}:${partyId}`;
        const entries = (await this.db.getAll('account_entries'))
            .filter((e) => e.party_key === key)
            .sort((a, b) => a.created_at.localeCompare(b.created_at));
        let bal = 0;
        
        let partyName = partyType === 'customer' ? 'عميل' : 'مورد';
        if (partyType === 'customer') {
            const c = await this.db.get('customers', partyId);
            partyName = c?.name || partyName;
        } else {
            const s = await this.db.get('suppliers', partyId);
            partyName = s?.name || partyName;
        }
        
        this.lastStatement = { partyType, partyId, key, entries };
        out.innerHTML = `<h4>كشف حساب: ${escapeHtml(partyName)}</h4><table class="table"><thead><tr><th>التاريخ</th><th>البيان</th><th>مدين</th><th>دائن</th><th>الرصيد</th></tr></thead><tbody>${
            entries.map((e) => {
                const delta = entryPartyRunningDelta(e);
                bal += delta;
                const dr = e.debit != null ? this.formatCurrency(e.debit) : '—';
                const cr = e.credit != null ? this.formatCurrency(e.credit) : '—';
                return `<tr><td>${e.created_at}</td><td>${escapeHtml(e.memo || '')}</td><td>${dr}</td><td>${cr}</td><td>${this.formatCurrency(delta)}</td><td>${this.formatCurrency(bal)}</td></tr>`;
            }).join('')
        }</tbody></table>`;
    }

    async printStatement() {
        if (!this.lastStatement || !this.lastStatement.entries?.length) {
            this.showToast('افتح كشف الحساب أولاً', 'warning');
            return;
        }
        const storeName = await this.getSetting('store_name', 'متجري');
        const phone = await this.getSetting('store_phone', '');
        const logo = await this.getSetting('store_logo', '');
        const logoImg = logo ? `<img src="${logo}" style="max-height:40px;max-width:80px;margin-bottom:10px;">` : '';
        
        let bal = 0;
        const rows = this.lastStatement.entries.map(e => {
            const delta = entryPartyRunningDelta(e);
            bal += delta;
            return `<tr><td>${new Date(e.created_at).toLocaleDateString('ar-SA')}</td><td>${escapeHtml(e.memo || '')}</td><td>${e.debit ? this.formatCurrency(e.debit) : '—'}</td><td>${e.credit ? this.formatCurrency(e.credit) : '—'}</td><td>${this.formatCurrency(delta)}</td><td>${this.formatCurrency(bal)}</td></tr>`;
        }).join('');
        
        const partyTypeLabel = this.lastStatement.partyType === 'customer' ? 'عميل' : 'مورد';
        let partyName = partyTypeLabel;
        if (this.lastStatement.partyType === 'customer') {
            const c = await this.db.get('customers', this.lastStatement.partyId);
            partyName = c?.name || partyTypeLabel;
        } else {
            const s = await this.db.get('suppliers', this.lastStatement.partyId);
            partyName = s?.name || partyTypeLabel;
        }
        const html = `<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
<meta charset="UTF-8">
<title>كشف حساب</title>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: Tahoma, Arial, sans-serif; width: 100%; margin: 0 auto; padding: 10mm; font-size: 11px; }
.header { text-align: center; margin-bottom: 15px; }
.header img { max-height: 40px; max-width: 80px; }
.header h2 { font-size: 16px; margin: 5px 0; }
table { width: 100%; border-collapse: collapse; margin: 10px 0; }
th, td { padding: 5px 3px; text-align: center; border: 1px solid #000; }
th { background: #f0f0f0; }
.footer { text-align: center; margin-top: 20px; color: #666; font-size: 10px; }
@media print { body { padding: 5mm; } }
</style>
</head>
<body>
<div class="header">
${logoImg}
<h2>${escapeHtml(storeName)}</h2>
<p>${escapeHtml(phone)}</p>
</div>
<h3 style="text-align:center;margin:10px 0;">كشف حساب ${escapeHtml(partyName)}</h3>
<table>
<thead><tr><th>التاريخ</th><th>البيان</th><th>مدين</th><th>دائن</th><th>الرصيد</th></tr></thead>
<tbody>${rows}</tbody>
</table>
<div class="footer">
<p>شكراً لتعاملكم معنا</p>
<p>تاريخ الطباعة: ${new Date().toLocaleString('ar-SA')}</p>
</div>
</body>
</html>`;
        
        const w = window.open('', '_blank');
        w.document.write(html);
        w.document.close();
        w.print();
    }

    async loadUsersTable() {
        const users = await this.db.getAll('users');
        const tbody = document.getElementById('users-table');
        if (!tbody) return;
        tbody.innerHTML = users.map((u) => `
            <tr>
                <td>${escapeHtml(u.username)}</td>
                <td>${escapeHtml(u.name)}</td>
                <td>${escapeHtml(u.role)}</td>
                <td>${u.active ? 'نشط' : 'موقوف'}</td>
                <td><button type="button" class="btn btn-sm btn-outline" data-user-action="edit" data-id="${u.id}">تعديل</button></td>
            </tr>`).join('');
    }

    openUserModal(u = null) {
        if (!this.hasPermission('manage_users')) return;
        const title = document.querySelector('#user-modal h3');
        document.getElementById('user-modal').classList.add('active');
        if (u) {
            if (title) title.textContent = 'تعديل مستخدم';
            document.getElementById('user-id').value = u.id;
            document.getElementById('user-username').value = u.username;
            document.getElementById('user-name').value = u.name;
            document.getElementById('user-role').value = u.role;
            document.getElementById('user-password').value = '';
            document.getElementById('user-active').checked = !!u.active;
        } else {
            if (title) title.textContent = 'مستخدم جديد';
            document.getElementById('user-form').reset();
            document.getElementById('user-id').value = '';
            document.getElementById('user-active').checked = true;
        }
    }

    async editUser(id) {
        const u = await this.db.get('users', id);
        if (u) this.openUserModal(u);
    }

    closeUserModal() {
        document.getElementById('user-modal').classList.remove('active');
    }

    async saveUser() {
        const id = document.getElementById('user-id').value;
        const username = document.getElementById('user-username').value.trim();
        const name = document.getElementById('user-name').value.trim();
        const role = document.getElementById('user-role').value;
        const password = document.getElementById('user-password').value;
        const active = document.getElementById('user-active').checked;
        if (!username || !name) {
            this.showToast('أكمل الحقول', 'error');
            return;
        }
        const all = await this.db.getAll('users');
        if (all.some((u) => u.username === username && String(u.id) !== String(id))) {
            this.showToast('اسم المستخدم مستخدم', 'error');
            return;
        }
        if (id) {
            const prev = await this.db.get('users', parseInt(id, 10));
            const row = { ...prev, username, name, role, active };
            if (password) row.password = await hashPassword(password);
            await this.db.put('users', row);
        } else {
            if (!password) {
                this.showToast('كلمة مرور للمستخدم الجديد', 'error');
                return;
            }
            await this.db.add('users', {
                username,
                name,
                role,
                active,
                password: await hashPassword(password)
            });
        }
        this.closeUserModal();
        this.loadUsersTable();
        this.showToast('تم حفظ المستخدم', 'success');
    }

    async exportBackup() {
        if (!this.hasPermission('manage_settings')) return;
        const data = {};
        const stores = ['products', 'invoices', 'expenses', 'customers', 'suppliers', 'users', 'activity', 'settings', 'account_entries', 'vouchers', 'meta'];
        for (const store of stores) {
            data[store] = await this.db.getAll(store);
        }
        const summary = stores.map(s => `${s}: ${data[s]?.length || 0}`).join(', ');
        if (!confirm(`تصدير نسخة احتياطية؟\n${summary}`)) return;
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
        const a = document.createElement('a');
        a.href = URL.createObjectURL(blob);
        a.download = `erp-backup-${new Date().toISOString().split('T')[0]}.json`;
        a.click();
        URL.revokeObjectURL(a.href);
        await this.db.setMeta('last_backup', new Date().toISOString());
        this.showToast('تم تصدير النسخة الاحتياطية', 'success');
    }

    async importBackup(file) {
        if (!file || !this.hasPermission('manage_settings')) return;
        const allowed = new Set(['products', 'invoices', 'expenses', 'customers', 'suppliers', 'users', 'activity', 'settings', 'account_entries', 'vouchers', 'meta']);
        let data;
        try {
            data = JSON.parse(await file.text());
        } catch {
            this.showToast('ملف النسخة غير صالح', 'error');
            return;
        }
        if (!data || typeof data !== 'object') {
            this.showToast('ملف النسخة غير صالح', 'error');
            return;
        }
        const summary = Object.keys(data).map(s => `${s}: ${data[s]?.length || 0}`).join(', ');
        if (!confirm(`استيراد النسخة الاحتياطية\nتحذير: سيتم استبدال جميع البيانات!\n${summary}\nمتابعة؟`)) return;
        try {
            for (const store of Object.keys(data)) {
                if (!allowed.has(store) || !Array.isArray(data[store])) continue;
                for (const row of data[store]) {
                    await this.db.put(store, row);
                }
            }
            this.invalidateCache();
            await this.logActivity('استعادة نسخة', `تم استيراد البيانات: ${summary}`);
            this.showToast('تم استعادة النسخة الاحتياطية', 'success');
        } catch (e) {
            console.error(e);
            this.showToast('فشل استعادة النسخة', 'error');
        }
    }

    async revertEntry(entryId) {
        const entry = await this.db.get('account_entries', entryId);
        if (!entry) {
            this.showToast('السجل غير موجود', 'error');
            return;
        }
        const canDelete = await this.canDeleteRecord('account_entries', entryId);
        if (!canDelete) {
            this.showToast('لا يمكن حذف إدخال محاسبي - استخدم العكس بدلاً من ذلك', 'error');
            return;
        }
        const refKey = entry.ref_key;
        if (!refKey) {
            this.showToast('لا يمكن عكس هذا السجل', 'error');
            return;
        }
        const allEntries = await this.db.getAll('account_entries');
        const related = allEntries.filter((e) => e.ref_key === refKey);
        if (!confirm(`عكس ${related.length} إدخال مرتبط بـ ${refKey}؟`)) return;
        try {
            const journalId = newJournalId();
            const reversalEntries = buildReversalJournal(journalId, related, new Date().toISOString(), 'عكس');
            for (const row of reversalEntries) {
                await this.db.add('account_entries', row);
            }
            await this.logActivity('عكس إدخالات', `تم عكس ${related.length} إدخال من ${refKey}`);
            this.showToast(`تم عكس ${related.length} إدخال`, 'success');
        } catch (e) {
            console.error(e);
            this.showToast('فشل العكس', 'error');
        }
    }

    // ===== دوال المشتريات =====

    async generatePurchaseInvoiceNumber() {
        const seq = (await this.db.getMeta('purchase_invoice_seq')) || 1;
        await this.db.setMeta('purchase_invoice_seq', seq + 1);
        return `PUR-${String(seq).padStart(5, '0')}`;
    }

    async savePurchaseInvoice() {
        const supplierId = parseInt(document.getElementById('purchase-supplier-id').value, 10);
        const paymentMethod = document.getElementById('purchase-payment-method').value;
        const paidAmount = parseFloat(document.getElementById('purchase-paid-amount').value) || 0;
        const notes = document.getElementById('purchase-notes').value;

        if (!supplierId) {
            this.showToast('اختر المورد', 'error');
            return;
        }

        const supplierRow = await this.db.get('suppliers', supplierId);
        if (!supplierRow) {
            this.showToast('المورد غير موجود', 'error');
            return;
        }

        if (!this.purchaseItems || this.purchaseItems.length === 0) {
            this.showToast('أضف منتجات للفاتورة', 'error');
            return;
        }

        const piNumber = await this.generatePurchaseInvoiceNumber();
        const now = new Date().toISOString();

        const prep = preparePurchaseInvoice({
            supplierId,
            supplierRow,
            items: this.purchaseItems,
            piNumber,
            paymentMethod,
            paidAmount,
            notes,
            currentUserName: this.currentUser.name,
            now
        });

        if (prep.error) {
            this.showToast(prep.error, 'error');
            return;
        }

        try {
            // تحديث المخزون
            for (const item of this.purchaseItems) {
                const product = await this.db.get('products', item.product_id);
                if (product) {
                    product.stock += item.quantity;
                    await this.db.put('products', product);
                }
            }

            // حفظ الفاتورة والقيود المحاسبية
            await this.db.executeWrites(prep.ops);

            // تحديث رصيد المورد
            await this.syncPartyBalanceFromGl('supplier', supplierId);

            // تسجيل النشاط
            await this.logActivity('فاتورة شراء', `تم إنشاء فاتورة شراء ${piNumber} من ${supplierRow.name} بقيمة ${this.formatCurrency(prep.pi.total)}`);

            this.showToast('تم حفظ فاتورة الشراء بنجاح', 'success');
            this.closePurchaseModal();
            this.purchaseItems = [];
            this.loadPurchaseInvoices();
            this.loadInventory();
        } catch (err) {
            this.showToast('خطأ: ' + err.message, 'error');
        }
    }

    async savePurchaseReturn() {
        const supplierId = parseInt(document.getElementById('return-supplier-id').value, 10);
        const piId = parseInt(document.getElementById('return-pi-id').value, 10);
        const notes = document.getElementById('return-notes').value;

        if (!supplierId || !piId) {
            this.showToast('اختر المورد وفاتورة الشراء', 'error');
            return;
        }

        const supplierRow = await this.db.get('suppliers', supplierId);
        if (!supplierRow) {
            this.showToast('المورد غير موجود', 'error');
            return;
        }

        if (!this.returnItems || this.returnItems.length === 0) {
            this.showToast('أضف منتجات للمرتجع', 'error');
            return;
        }

        const prNumber = `RET-${String(Date.now()).slice(-8)}`;
        const now = new Date().toISOString();

        const prep = preparePurchaseReturn({
            supplierId,
            supplierRow,
            piId,
            items: this.returnItems,
            prNumber,
            notes,
            currentUserName: this.currentUser.name,
            now
        });

        if (prep.error) {
            this.showToast(prep.error, 'error');
            return;
        }

        try {
            // تحديث المخزون (تقليل الكمية)
            for (const item of this.returnItems) {
                const product = await this.db.get('products', item.product_id);
                if (product) {
                    product.stock = Math.max(0, product.stock - item.quantity);
                    await this.db.put('products', product);
                }
            }

            // حفظ المرتجع والقيود المحاسبية
            await this.db.executeWrites(prep.ops);

            // تحديث رصيد المورد
            await this.syncPartyBalanceFromGl('supplier', supplierId);

            // تسجيل النشاط
            await this.logActivity('مرتجع شراء', `تم إنشاء مرتجع شراء ${prNumber} بقيمة ${this.formatCurrency(prep.pr.total)}`);

            this.showToast('تم حفظ مرتجع الشراء بنجاح', 'success');
            this.closeReturnModal();
            this.returnItems = [];
            this.loadPurchaseReturns();
            this.loadInventory();
        } catch (err) {
            this.showToast('خطأ: ' + err.message, 'error');
        }
    }

    async loadPurchasingScreen() {
        await this.loadPurchaseInvoices();
        await this.loadPurchaseReturns();
        
        // تحميل الموردين في الاختيارات
        const suppliers = await this.db.getAll('suppliers');
        const supplierSelect = document.getElementById('purchase-supplier-id');
        const returnSupplierSelect = document.getElementById('return-supplier-id');
        if (supplierSelect) {
            supplierSelect.innerHTML = '<option value="">اختر المورد</option>' + suppliers.map((s) => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
        }
        if (returnSupplierSelect) {
            returnSupplierSelect.innerHTML = '<option value="">اختر المورد</option>' + suppliers.map((s) => `<option value="${s.id}">${escapeHtml(s.name)}</option>`).join('');
        }
        
        // تحميل المنتجات في الاختيارات
        const products = await this.getProducts(true);
        const productSelect = document.getElementById('purchase-product-id');
        const returnProductSelect = document.getElementById('return-product-id');
        if (productSelect) {
            productSelect.innerHTML = '<option value="">اختر المنتج</option>' + products.map((p) => `<option value="${p.id}">${escapeHtml(p.name)}</option>`).join('');
        }
        if (returnProductSelect) {
            returnProductSelect.innerHTML = '<option value="">اختر المنتج</option>' + products.map((p) => `<option value="${p.id}">${escapeHtml(p.name)}</option>`).join('');
        }
        
        // تحميل فواتير الشراء في اختيار المرتجعات
        const invoices = await this.db.getAll('purchase_invoices');
        const piSelect = document.getElementById('return-pi-id');
        if (piSelect) {
            piSelect.innerHTML = '<option value="">اختر فاتورة الشراء</option>' + invoices.map((pi) => `<option value="${pi.id}">${escapeHtml(pi.pi_number)} - ${escapeHtml(pi.supplier_name)}</option>`).join('');
        }
    }

    async loadPurchaseInvoices() {
        const invoices = (await this.db.getAll('purchase_invoices')).slice().reverse().slice(0, 100);
        const tbody = document.getElementById('purchase-invoices-tbody');
        if (!tbody) return;

        if (invoices.length === 0) {
            tbody.innerHTML = '<tr><td colspan="6" class="empty-state">لا توجد فواتير شراء</td></tr>';
        } else {
            tbody.innerHTML = invoices.map((pi) => `
                <tr>
                    <td>${escapeHtml(pi.pi_number)}</td>
                    <td>${escapeHtml(pi.supplier_name)}</td>
                    <td>${this.formatCurrency(pi.total)}</td>
                    <td><span class="badge ${pi.status === 'paid' ? 'badge-success' : 'badge-warning'}">${pi.status === 'paid' ? 'مدفوعة' : 'معلقة'}</span></td>
                    <td>${new Date(pi.created_at).toLocaleDateString('ar-SA')}</td>
                    <td>
                        <button type="button" class="btn btn-sm btn-outline" data-pi-action="view" data-id="${pi.id}"><i class="fas fa-eye"></i></button>
                    </td>
                </tr>
            `).join('');
        }
    }

    async loadPurchaseReturns() {
        const returns = (await this.db.getAll('purchase_returns')).slice().reverse().slice(0, 100);
        const tbody = document.getElementById('purchase-returns-tbody');
        if (!tbody) return;

        if (returns.length === 0) {
            tbody.innerHTML = '<tr><td colspan="5" class="empty-state">لا توجد مرتجعات شراء</td></tr>';
        } else {
            tbody.innerHTML = returns.map((pr) => `
                <tr>
                    <td>${escapeHtml(pr.pr_number)}</td>
                    <td>${escapeHtml(pr.supplier_name)}</td>
                    <td>${this.formatCurrency(pr.total)}</td>
                    <td>${new Date(pr.created_at).toLocaleDateString('ar-SA')}</td>
                    <td>
                        <button type="button" class="btn btn-sm btn-outline" data-pr-action="view" data-id="${pr.id}"><i class="fas fa-eye"></i></button>
                    </td>
                </tr>
            `).join('');
        }
    }

    addPurchaseItem() {
        const productId = parseInt(document.getElementById('purchase-product-id').value, 10);
        const quantity = parseInt(document.getElementById('purchase-quantity').value, 10) || 0;
        const unitPrice = parseFloat(document.getElementById('purchase-unit-price').value) || 0;

        if (!productId || quantity <= 0 || unitPrice <= 0) {
            this.showToast('أدخل بيانات صحيحة', 'error');
            return;
        }

        this.db.get('products', productId).then((product) => {
            if (!product) {
                this.showToast('المنتج غير موجود', 'error');
                return;
            }

            const existingItem = this.purchaseItems.find((item) => item.product_id === productId);
            if (existingItem) {
                existingItem.quantity += quantity;
            } else {
                this.purchaseItems.push({
                    product_id: productId,
                    product_name: product.name,
                    quantity,
                    unit_price: unitPrice,
                    total: quantity * unitPrice
                });
            }

            document.getElementById('purchase-product-id').value = '';
            document.getElementById('purchase-quantity').value = '';
            document.getElementById('purchase-unit-price').value = '';
            this.updatePurchaseItemsDisplay();
        });
    }

    removePurchaseItem(index) {
        this.purchaseItems.splice(index, 1);
        this.updatePurchaseItemsDisplay();
    }

    updatePurchaseItemsDisplay() {
        const container = document.getElementById('purchase-items-list');
        if (!container) return;

        const total = computePurchaseTotal(this.purchaseItems);
        container.innerHTML = this.purchaseItems.map((item, idx) => `
            <div class="purchase-item" style="padding: 8px; border: 1px solid var(--border); margin-bottom: 8px; border-radius: 4px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span>${escapeHtml(item.product_name)}</span>
                    <span>${item.quantity} × ${this.formatCurrency(item.unit_price)} = ${this.formatCurrency(item.total)}</span>
                    <button type="button" class="btn btn-sm btn-danger" onclick="app.removePurchaseItem(${idx})">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
        `).join('');

        const totalEl = document.getElementById('purchase-total');
        if (totalEl) totalEl.textContent = this.formatCurrency(total);
    }

    openPurchaseModal() {
        this.purchaseItems = [];
        document.getElementById('purchase-form').reset();
        document.getElementById('purchase-modal').classList.add('active');
        this.updatePurchaseItemsDisplay();
    }

    closePurchaseModal() {
        document.getElementById('purchase-modal').classList.remove('active');
    }

    openReturnModal() {
        this.returnItems = [];
        document.getElementById('return-form').reset();
        document.getElementById('return-modal').classList.add('active');
    }

    addReturnItem() {
        const productId = parseInt(document.getElementById('return-product-id').value, 10);
        const quantity = parseInt(document.getElementById('return-quantity').value, 10) || 0;

        if (!productId || quantity <= 0) {
            this.showToast('أدخل بيانات صحيحة', 'error');
            return;
        }

        this.db.get('products', productId).then((product) => {
            if (!product) {
                this.showToast('المنتج غير موجود', 'error');
                return;
            }

            const existingItem = this.returnItems.find((item) => item.product_id === productId);
            if (existingItem) {
                existingItem.quantity += quantity;
            } else {
                this.returnItems.push({
                    product_id: productId,
                    product_name: product.name,
                    quantity,
                    unit_price: product.cost_price,
                    total: quantity * product.cost_price
                });
            }

            document.getElementById('return-product-id').value = '';
            document.getElementById('return-quantity').value = '1';
            this.updateReturnItemsDisplay();
        });
    }

    removeReturnItem(index) {
        this.returnItems.splice(index, 1);
        this.updateReturnItemsDisplay();
    }

    updateReturnItemsDisplay() {
        const container = document.getElementById('return-items-list');
        if (!container) return;

        const total = computePurchaseTotal(this.returnItems);
        container.innerHTML = this.returnItems.map((item, idx) => `
            <div class="return-item" style="padding: 8px; border: 1px solid var(--border); margin-bottom: 8px; border-radius: 4px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span>${escapeHtml(item.product_name)}</span>
                    <span>${item.quantity} × ${this.formatCurrency(item.unit_price)} = ${this.formatCurrency(item.total)}</span>
                    <button type="button" class="btn btn-sm btn-danger" onclick="app.removeReturnItem(${idx})">
                        <i class="fas fa-trash"></i>
                    </button>
                </div>
            </div>
        `).join('');
    }

    closeReturnModal() {
        document.getElementById('return-modal').classList.remove('active');
    }
}
