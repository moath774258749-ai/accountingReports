/** @typedef {'admin'|'manager'|'cashier'|'developer'} Role */

export const CONFIG = {
    CURRENCY: 'ريال',
    UNIT_CONVERSION: {
        box: 50,
        pack: 10
    },
    EXPENSE_CATEGORIES: {
        electricity: 'كهرباء',
        water: 'مياه',
        rent: 'إيجار',
        supplies: 'مستلزمات',
        maintenance: 'صيانة',
        transport: 'نقل',
        other: 'أخرى'
    }
};

export const ROLES = {
    ADMIN: 'admin',
    MANAGER: 'manager',
    CASHIER: 'cashier',
    DEVELOPER: 'developer'
};

/** Admin-only: view_cost, view_reports, manage_users. Manager: operations without cost/reports/user admin. Manager: can manage products. Cashier: POS + vouchers + statements. Developer: system initialization and reset. */
export const PERMISSIONS = {
    admin: ['view_reports', 'view_cost', 'manage_users', 'manage_settings', 'manage_products', 'delete_records', 'view_all'],
    manager: ['manage_products'],
    cashier: ['pos_only', 'create_vouchers', 'print_statements'],
    developer: ['system_init', 'reset_database', 'view_all', 'manage_users']
};

export const SESSION_TTL_MS = 12 * 60 * 60 * 1000;
export const LOCK_PERIOD_DAYS = 30;
export const BACKUP_REMINDER_DAYS = 7;
