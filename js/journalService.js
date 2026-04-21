import { AC } from './chartOfAccounts.js';

export function newJournalId() {
    return `JRNL-${crypto.randomUUID().replace(/-/g, '').slice(0, 12)}`;
}

/**
 * @param {{ debit?: number, credit?: number }[]} lines
 */
export function assertBalanced(lines) {
    const d = lines.reduce((s, l) => s + (Number(l.debit) || 0), 0);
    const c = lines.reduce((s, l) => s + (Number(l.credit) || 0), 0);
    if (Math.abs(d - c) > 0.001) {
        throw new Error(`Journal not balanced: debit=${d} credit=${c}`);
    }
}

/**
 * Signed effect on party sub-ledger (customer AR or supplier AP) for statement running total.
 * AR (asset): debit increases balance owed to us → D - C
 * AP (liability): credit increases what we owe → C - D
 */
export function partySubledgerAmount(line) {
    if (!line.party_key) return undefined;
    if (line.account_code === AC.AP) {
        return (Number(line.credit) || 0) - (Number(line.debit) || 0);
    }
    return (Number(line.debit) || 0) - (Number(line.credit) || 0);
}

/** Running balance on party sub-ledger (legacy `amount` or debit/credit on AR/AP). */
export function entryPartyRunningDelta(e) {
    if (!e.party_key) return 0;
    const hasDc = e.debit != null || e.credit != null;
    if (!hasDc && e.amount != null) return Number(e.amount) || 0;
    if (e.account_code === AC.AP) return (Number(e.credit) || 0) - (Number(e.debit) || 0);
    return (Number(e.debit) || 0) - (Number(e.credit) || 0);
}

/**
 * @param {string} journalId
 * @param {object[]} lines — { account_code, debit, credit, party_key?, entry_kind, memo }
 * @param {string} ref_type
 * @param {string|number} ref_id
 * @param {string} ref_key
 * @param {string} now ISO
 */
export function journalLinesToAccountEntries(journalId, lines, ref_type, ref_id, ref_key, now) {
    return lines.map((l) => {
        const debit = Number(l.debit) || 0;
        const credit = Number(l.credit) || 0;
        const row = {
            journal_id: journalId,
            account_code: l.account_code,
            debit,
            credit,
            party_key: l.party_key || undefined,
            ref_type,
            ref_id,
            ref_key,
            entry_kind: l.entry_kind,
            memo: l.memo || '',
            created_at: now
        };
        const pa = partySubledgerAmount({ ...l, debit, credit });
        if (l.party_key && pa !== undefined) row.amount = pa;
        return row;
    });
}

/**
 * @returns {object[]} account_entries rows (not yet wrapped in {type:'add',store})
 */
export function buildSaleJournal(journalId, p) {
    const {
        invoiceNumber,
        subtotal,
        discount,
        paid,
        due,
        cost,
        customerId,
        now
    } = p;
    const refKey = `invoice:${invoiceNumber}`;
    const lines = [];

    if (paid > 0) {
        lines.push({
            account_code: AC.CASH,
            debit: paid,
            credit: 0,
            entry_kind: 'sale_cash',
            memo: `قبض نقدي — فاتورة ${invoiceNumber}`
        });
    }
    if (due > 0 && customerId) {
        lines.push({
            account_code: AC.AR,
            debit: due,
            credit: 0,
            party_key: `customer:${customerId}`,
            entry_kind: 'sale_ar',
            memo: `ذمم مدينة — فاتورة ${invoiceNumber}`
        });
    }
    if (discount > 0) {
        lines.push({
            account_code: AC.SALES_DIS,
            debit: discount,
            credit: 0,
            entry_kind: 'sales_discount',
            memo: `خصم — فاتورة ${invoiceNumber}`
        });
    }
    lines.push({
        account_code: AC.SALES,
        debit: 0,
        credit: subtotal,
        entry_kind: 'sale_revenue',
        memo: `إيراد بيع — فاتورة ${invoiceNumber}`
    });

    if (cost > 0) {
        lines.push({
            account_code: AC.COGS,
            debit: cost,
            credit: 0,
            entry_kind: 'cogs',
            memo: `تكلفة — فاتورة ${invoiceNumber}`
        });
        lines.push({
            account_code: AC.INV,
            debit: 0,
            credit: cost,
            entry_kind: 'inv_issue',
            memo: `تخفيض مخزون — فاتورة ${invoiceNumber}`
        });
    }

    assertBalanced(lines);
    return journalLinesToAccountEntries(journalId, lines, 'invoice', invoiceNumber, refKey, now);
}

export function buildReturnJournal(journalId, p) {
    const {
        invoiceNumber,
        subtotal,
        discount,
        paid,
        due,
        cost,
        customerId,
        now
    } = p;
    const refKey = `invoice:${invoiceNumber}`;
    const lines = [];

    lines.push({
        account_code: AC.SALES,
        debit: subtotal,
        credit: 0,
        entry_kind: 'return_revenue',
        memo: `عكس إيراد — مرتجع ${invoiceNumber}`
    });
    if (discount > 0) {
        lines.push({
            account_code: AC.SALES_DIS,
            debit: 0,
            credit: discount,
            entry_kind: 'return_discount',
            memo: `عكس خصم — مرتجع ${invoiceNumber}`
        });
    }
    if (paid > 0) {
        lines.push({
            account_code: AC.CASH,
            debit: 0,
            credit: paid,
            entry_kind: 'return_cash',
            memo: `رد نقدي — مرتجع ${invoiceNumber}`
        });
    }
    if (due > 0 && customerId) {
        lines.push({
            account_code: AC.AR,
            debit: 0,
            credit: due,
            party_key: `customer:${customerId}`,
            entry_kind: 'return_ar',
            memo: `تخفيض ذمم — مرتجع ${invoiceNumber}`
        });
    }

    if (cost > 0) {
        lines.push({
            account_code: AC.INV,
            debit: cost,
            credit: 0,
            entry_kind: 'inv_return',
            memo: `إرجاع مخزون — مرتجع ${invoiceNumber}`
        });
        lines.push({
            account_code: AC.COGS,
            debit: 0,
            credit: cost,
            entry_kind: 'cogs_return',
            memo: `عكس تكلفة — مرتجع ${invoiceNumber}`
        });
    }

    assertBalanced(lines);
    return journalLinesToAccountEntries(journalId, lines, 'invoice', invoiceNumber, refKey, now);
}

export function buildExpenseJournal(journalId, p) {
    const { amount, refKey, refId, memo, now } = p;
    const lines = [
        {
            account_code: AC.EXP,
            debit: amount,
            credit: 0,
            entry_kind: 'expense',
            memo: memo || 'مصروف'
        },
        {
            account_code: AC.CASH,
            debit: 0,
            credit: amount,
            entry_kind: 'expense_cash',
            memo: memo || 'سداد نقدي — مصروف'
        }
    ];
    assertBalanced(lines);
    return journalLinesToAccountEntries(journalId, lines, 'expense', refId, refKey, now);
}

export function buildReceiptVoucherJournal(journalId, p) {
    const { amount, partyId, voucherNumber, memo, now } = p;
    const refKey = `voucher:${voucherNumber}`;
    const lines = [
        {
            account_code: AC.CASH,
            debit: amount,
            credit: 0,
            entry_kind: 'receipt_cash',
            memo: memo || `سند قبض ${voucherNumber}`
        },
        {
            account_code: AC.AR,
            debit: 0,
            credit: amount,
            party_key: `customer:${partyId}`,
            entry_kind: 'receipt_ar',
            memo: memo || `سند قبض ${voucherNumber}`
        }
    ];
    assertBalanced(lines);
    return journalLinesToAccountEntries(journalId, lines, 'voucher', voucherNumber, refKey, now);
}

export function buildSupplierPaymentJournal(journalId, p) {
    const { amount, partyId, voucherNumber, memo, now } = p;
    const refKey = `voucher:${voucherNumber}`;
    const lines = [
        {
            account_code: AC.AP,
            debit: amount,
            credit: 0,
            party_key: `supplier:${partyId}`,
            entry_kind: 'payment_ap',
            memo: memo || `سند صرف ${voucherNumber}`
        },
        {
            account_code: AC.CASH,
            debit: 0,
            credit: amount,
            entry_kind: 'payment_cash',
            memo: memo || `سند صرف ${voucherNumber}`
        }
    ];
    assertBalanced(lines);
    return journalLinesToAccountEntries(journalId, lines, 'voucher', voucherNumber, refKey, now);
}

export function buildReversalJournal(journalId, entriesToReverse, now, reason) {
    const refKey = entriesToReverse[0]?.ref_key || 'reversal';
    const refType = entriesToReverse[0]?.ref_type || 'reversal';
    const lines = entriesToReverse.map((e) => ({
        account_code: e.account_code,
        debit: Number(e.credit) || 0,
        credit: Number(e.debit) || 0,
        party_key: e.party_key,
        entry_kind: 'reversal',
        memo: `عكس - ${reason || e.memo || ''}`
    }));
    assertBalanced(lines);
    return journalLinesToAccountEntries(journalId, lines, refType, 'reversal', refKey + '-REV', now);
}
