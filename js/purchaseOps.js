/**
 * Purchase Operations: POs, Purchase Invoices, Returns
 * Handles accounting journal entries and inventory updates
 */

import { AC } from './chartOfAccounts.js';
import { newJournalId } from './journalService.js';

/**
 * Build purchase invoice journal entries
 * Debit: Inventory (COGS) | Credit: Accounts Payable (supplier)
 */
export function buildPurchaseInvoiceJournal(journalId, { amount, supplierPartyKey, piNumber, memo, now }) {
    return [
        {
            journal_id: journalId,
            account_code: AC.INVENTORY,
            debit: amount,
            credit: 0,
            party_key: null,
            description: `فاتورة شراء ${piNumber}`,
            ref_key: `purchase_invoice:${piNumber}`,
            created_at: now
        },
        {
            journal_id: journalId,
            account_code: AC.PAYABLE,
            debit: 0,
            credit: amount,
            party_key: supplierPartyKey,
            description: `فاتورة شراء ${piNumber}`,
            ref_key: `purchase_invoice:${piNumber}`,
            created_at: now
        }
    ];
}

/**
 * Build purchase return journal entries
 * Debit: Accounts Payable (supplier) | Credit: Inventory (COGS)
 */
export function buildPurchaseReturnJournal(journalId, { amount, supplierPartyKey, prNumber, memo, now }) {
    return [
        {
            journal_id: journalId,
            account_code: AC.PAYABLE,
            debit: amount,
            credit: 0,
            party_key: supplierPartyKey,
            description: `مرتجع شراء ${prNumber}`,
            ref_key: `purchase_return:${prNumber}`,
            created_at: now
        },
        {
            journal_id: journalId,
            account_code: AC.INVENTORY,
            debit: 0,
            credit: amount,
            party_key: null,
            description: `مرتجع شراء ${prNumber}`,
            ref_key: `purchase_return:${prNumber}`,
            created_at: now
        }
    ];
}

/**
 * Build cash payment for purchase invoice
 * Debit: Accounts Payable | Credit: Cash
 */
export function buildPurchasePaymentJournal(journalId, { amount, supplierPartyKey, piNumber, now }) {
    return [
        {
            journal_id: journalId,
            account_code: AC.PAYABLE,
            debit: amount,
            credit: 0,
            party_key: supplierPartyKey,
            description: `دفع فاتورة شراء ${piNumber}`,
            ref_key: `purchase_payment:${piNumber}`,
            created_at: now
        },
        {
            journal_id: journalId,
            account_code: AC.CASH,
            debit: 0,
            credit: amount,
            party_key: null,
            description: `دفع فاتورة شراء ${piNumber}`,
            ref_key: `purchase_payment:${piNumber}`,
            created_at: now
        }
    ];
}

/**
 * Calculate total cost of purchase items
 */
export function computePurchaseTotal(items) {
    return items.reduce((sum, item) => sum + item.quantity * item.unit_price, 0);
}

/**
 * Validate purchase invoice
 */
export function validatePurchaseInvoice(piData) {
    if (!piData.supplier_id || piData.supplier_id <= 0) {
        return { error: 'اختر المورد' };
    }
    if (!piData.items || piData.items.length === 0) {
        return { error: 'أضف منتجات للفاتورة' };
    }
    if (piData.total <= 0) {
        return { error: 'الإجمالي يجب أن يكون أكبر من صفر' };
    }
    return { error: null };
}

/**
 * Validate purchase return
 */
export function validatePurchaseReturn(prData) {
    if (!prData.supplier_id || prData.supplier_id <= 0) {
        return { error: 'اختر المورد' };
    }
    if (!prData.pi_id || prData.pi_id <= 0) {
        return { error: 'اختر فاتورة الشراء' };
    }
    if (!prData.items || prData.items.length === 0) {
        return { error: 'أضف منتجات للمرتجع' };
    }
    if (prData.total <= 0) {
        return { error: 'الإجمالي يجب أن يكون أكبر من صفر' };
    }
    return { error: null };
}

/**
 * Prepare purchase invoice for saving
 * Returns: { error, ops, pi }
 */
export function preparePurchaseInvoice({
    supplierId,
    supplierRow,
    items,
    piNumber,
    paymentMethod,
    paidAmount,
    notes,
    currentUserName,
    now
}) {
    const validation = validatePurchaseInvoice({ supplier_id: supplierId, items, total: computePurchaseTotal(items) });
    if (validation.error) return validation;

    const total = computePurchaseTotal(items);
    const pi = {
        pi_number: piNumber,
        supplier_id: supplierId,
        supplier_name: supplierRow.name,
        items,
        subtotal: total,
        total,
        payment_method: paymentMethod,
        paid_amount: paymentMethod === 'cash' ? paidAmount : 0,
        status: paymentMethod === 'cash' ? 'paid' : 'pending',
        notes,
        created_by: currentUserName,
        created_at: now
    };

    const journalId = newJournalId();
    const supplierPartyKey = `supplier:${supplierId}`;
    const glRows = buildPurchaseInvoiceJournal(journalId, {
        amount: total,
        supplierPartyKey,
        piNumber,
        memo: notes,
        now
    });

    const ops = [
        { type: 'add', store: 'purchase_invoices', value: pi },
        ...glRows.map((row) => ({ type: 'add', store: 'account_entries', value: row }))
    ];

    // إذا كانت نقداً، أضف قيد الدفع
    if (paymentMethod === 'cash' && paidAmount > 0) {
        const paymentJournal = buildPurchasePaymentJournal(journalId, {
            amount: paidAmount,
            supplierPartyKey,
            piNumber,
            now
        });
        ops.push(...paymentJournal.map((row) => ({ type: 'add', store: 'account_entries', value: row })));
    }

    return { error: null, ops, pi };
}

/**
 * Prepare purchase return for saving
 */
export function preparePurchaseReturn({
    supplierId,
    supplierRow,
    piId,
    items,
    prNumber,
    notes,
    currentUserName,
    now
}) {
    const validation = validatePurchaseReturn({ supplier_id: supplierId, pi_id: piId, items, total: computePurchaseTotal(items) });
    if (validation.error) return validation;

    const total = computePurchaseTotal(items);
    const pr = {
        pr_number: prNumber,
        supplier_id: supplierId,
        supplier_name: supplierRow.name,
        pi_id: piId,
        items,
        total,
        notes,
        created_by: currentUserName,
        created_at: now
    };

    const journalId = newJournalId();
    const supplierPartyKey = `supplier:${supplierId}`;
    const glRows = buildPurchaseReturnJournal(journalId, {
        amount: total,
        supplierPartyKey,
        prNumber,
        memo: notes,
        now
    });

    const ops = [
        { type: 'add', store: 'purchase_returns', value: pr },
        ...glRows.map((row) => ({ type: 'add', store: 'account_entries', value: row }))
    ];

    return { error: null, ops, pr };
}
