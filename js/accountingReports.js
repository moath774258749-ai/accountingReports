import { AC, CHART_LABELS, CHART_META, isCreditNormalAccountType } from './chartOfAccounts.js';
import { entryPartyRunningDelta } from './journalService.js';

/** Row has GL coding suitable for TB / P&L / ledger */
export function isCodedGlEntry(e) {
    return Boolean(e?.account_code) && (e.debit != null || e.credit != null);
}

/**
 * @param {object[]} entries
 * @param {(iso: string, from?: string, to?: string) => boolean} inRange
 */
export function filterEntriesByDate(entries, from, to, inRange) {
    return entries.filter((e) => inRange(e.created_at, from, to));
}

/** Cumulative through end date (YYYY-MM-DD), inclusive. If `to` falsy, no date filter. */
export function filterEntriesAsOfTo(entries, to) {
    if (!to) return entries;
    const t = String(to).split('T')[0];
    return entries.filter((e) => {
        const d = (e.created_at || '').split('T')[0];
        return d <= t;
    });
}

/**
 * Net balance for one GL account (coded lines only). Sign from CHART_META.
 */
export function netBalanceForAccount(entries, accountCode) {
    const meta = CHART_META[accountCode];
    if (!meta) return 0;
    const list = entries.filter((e) => e.account_code === accountCode && isCodedGlEntry(e));
    const dr = list.reduce((s, e) => s + (Number(e.debit) || 0), 0);
    const cr = list.reduce((s, e) => s + (Number(e.credit) || 0), 0);
    if (isCreditNormalAccountType(meta.type)) return cr - dr;
    return dr - cr;
}

/**
 * Simple balance sheet (point in time = all coded entries through `asOfTo`).
 */
export function buildSimpleBalanceSheet(entriesAsOf) {
    const cash = netBalanceForAccount(entriesAsOf, AC.CASH);
    const inv = netBalanceForAccount(entriesAsOf, AC.INV);
    const ar = netBalanceForAccount(entriesAsOf, AC.AR);
    const assets = cash + inv + ar;
    const ap = netBalanceForAccount(entriesAsOf, AC.AP);
    const liabilities = ap;
    const equity = assets - liabilities;
    return { cash, inv, ar, assets, ap, liabilities, equity };
}

/** Sum sub-ledger effect for party_key (includes legacy rows with `amount` only). */
export function sumPartySubledger(entries, partyKey) {
    if (!partyKey) return 0;
    return entries
        .filter((e) => e.party_key === partyKey)
        .reduce((s, e) => s + entryPartyRunningDelta(e), 0);
}

/** party_key -> running sum (for batching UI reads). */
export function partySubledgerTotalsMap(entries) {
    const m = new Map();
    for (const e of entries) {
        if (!e.party_key) continue;
        const k = e.party_key;
        m.set(k, (m.get(k) || 0) + entryPartyRunningDelta(e));
    }
    return m;
}

/**
 * Trial balance: group by account_code, sum debit/credit.
 * @returns {{ rows: { code: string, label: string, debit: number, credit: number }[], totalDebit: number, totalCredit: number, balanced: boolean, excludedLegacyCount: number }}
 */
export function buildTrialBalance(entries) {
    const map = {};
    let excludedLegacyCount = 0;

    for (const e of entries) {
        if (!isCodedGlEntry(e)) {
            excludedLegacyCount++;
            continue;
        }
        const code = e.account_code;
        if (!map[code]) {
            map[code] = { code, label: CHART_LABELS[code] || code, debit: 0, credit: 0 };
        }
        map[code].debit += Number(e.debit) || 0;
        map[code].credit += Number(e.credit) || 0;
    }

    const rows = Object.values(map).sort((a, b) => a.code.localeCompare(b.code));
    const totalDebit = rows.reduce((s, r) => s + r.debit, 0);
    const totalCredit = rows.reduce((s, r) => s + r.credit, 0);
    const balanced = Math.abs(totalDebit - totalCredit) < 0.001;

    return {
        rows,
        totalDebit,
        totalCredit,
        balanced,
        excludedLegacyCount
    };
}

/**
 * P&L from posted accounts (SALES, SALES_DIS, COGS, EXP).
 * Sales: credit − debit (revenue). Discount / COGS / Exp: debit − credit (cost/expense character).
 */
export function buildProfitAndLoss(entries) {
    const pick = (code) => entries.filter((e) => e.account_code === code && isCodedGlEntry(e));
    const sumD = (list) => list.reduce((s, e) => s + (Number(e.debit) || 0), 0);
    const sumC = (list) => list.reduce((s, e) => s + (Number(e.credit) || 0), 0);

    const salesList = pick(AC.SALES);
    const disList = pick(AC.SALES_DIS);
    const cogsList = pick(AC.COGS);
    const expList = pick(AC.EXP);

    const salesGross = sumC(salesList) - sumD(salesList);
    const salesDiscounts = sumD(disList) - sumC(disList);
    const netSales = salesGross - salesDiscounts;
    const cogsExpense = sumD(cogsList) - sumC(cogsList);
    const expenseTotal = sumD(expList) - sumC(expList);

    const grossProfit = netSales - cogsExpense;
    const netProfit = grossProfit - expenseTotal;

    return {
        salesGross,
        salesDiscounts,
        netSales,
        cogsExpense,
        expenseTotal,
        grossProfit,
        netProfit
    };
}

/**
 * General ledger lines for one account, chronological, with running balance.
 * @param {object[]} entries — pre-filtered by date if needed
 * @param {string} accountCode
 */
export function buildGeneralLedger(entries, accountCode) {
    const meta = CHART_META[accountCode];
    const creditNormal = meta ? isCreditNormalAccountType(meta.type) : false;

    const list = entries
        .filter((e) => e.account_code === accountCode && isCodedGlEntry(e))
        .sort((a, b) => {
            const t = String(a.created_at || '').localeCompare(String(b.created_at || ''));
            if (t !== 0) return t;
            return (Number(a.id) || 0) - (Number(b.id) || 0);
        });

    let running = 0;
    return list.map((e) => {
        const dr = Number(e.debit) || 0;
        const cr = Number(e.credit) || 0;
        const delta = creditNormal ? cr - dr : dr - cr;
        running += delta;
        return {
            id: e.id,
            created_at: e.created_at,
            memo: e.memo || '',
            ref_key: e.ref_key || '',
            entry_kind: e.entry_kind || '',
            debit: dr,
            credit: cr,
            delta,
            running
        };
    });
}

export function ledgerAccountOptions() {
    return Object.keys(CHART_LABELS)
        .sort()
        .map((code) => ({ code, label: CHART_LABELS[code] || code }));
}
