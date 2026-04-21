/**
 * Basic chart of accounts (codes used in journal lines).
 * AP included for supplier payments (double-entry completeness).
 */
export const AC = {
    CASH: 'CASH',
    INV: 'INV',
    SALES: 'SALES',
    COGS: 'COGS',
    AR: 'AR',
    AP: 'AP',
    EXP: 'EXP',
    /** Contra-revenue: promotional discount on invoice */
    SALES_DIS: 'SALES_DIS'
};

/** For sign rules in BS / ledger running balance (keep minimal). */
export const ACCOUNT_TYPE = {
    ASSET: 'asset',
    LIABILITY: 'liability',
    REVENUE: 'revenue',
    EXPENSE: 'expense'
};

/**
 * @typedef {'asset'|'liability'|'revenue'|'expense'} AccountKind
 * @type {Record<string, { type: AccountKind }>}
 */
export const CHART_META = {
    [AC.CASH]: { type: ACCOUNT_TYPE.ASSET },
    [AC.INV]: { type: ACCOUNT_TYPE.ASSET },
    [AC.AR]: { type: ACCOUNT_TYPE.ASSET },
    [AC.AP]: { type: ACCOUNT_TYPE.LIABILITY },
    [AC.SALES]: { type: ACCOUNT_TYPE.REVENUE },
    [AC.SALES_DIS]: { type: ACCOUNT_TYPE.EXPENSE },
    [AC.COGS]: { type: ACCOUNT_TYPE.EXPENSE },
    [AC.EXP]: { type: ACCOUNT_TYPE.EXPENSE }
};

/** Revenue & liability: credit − debit for running net. Asset & expense: debit − credit. */
export function isCreditNormalAccountType(type) {
    return type === ACCOUNT_TYPE.REVENUE || type === ACCOUNT_TYPE.LIABILITY;
}

export const CHART_LABELS = {
    [AC.CASH]: 'الصندوق / النقدية',
    [AC.INV]: 'المخزون',
    [AC.SALES]: 'المبيعات',
    [AC.COGS]: 'تكلفة البضاعة المباعة',
    [AC.AR]: 'العملاء (ذمم مدينة)',
    [AC.AP]: 'الموردون (ذمم دائنة)',
    [AC.EXP]: 'المصروفات',
    [AC.SALES_DIS]: 'خصومات المبيعات'
};
