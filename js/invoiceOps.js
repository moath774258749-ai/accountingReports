import { CONFIG } from './constants.js';
import { getUnitMultiplier, lineCost, lineRevenue } from './units.js';
import { newJournalId, buildSaleJournal, buildReturnJournal } from './journalService.js';

export function clampDiscount(subtotal, discount) {
    const d = Math.max(0, Number(discount) || 0);
    return Math.min(d, subtotal);
}

export function effectivePaidAmount(paymentMethod, total, paidInput) {
    if (paymentMethod === 'cash') return total;
    if (paymentMethod === 'credit') return 0;
    const p = Math.max(0, Number(paidInput) || 0);
    return Math.min(p, total);
}

/**
 * @param {object} params
 * @returns {{ error?: string, ops?: object[], invoice?: object }}
 */
export function prepareCompleteSale(params) {
    const {
        cart,
        productsById,
        invoiceType,
        invoiceNumber,
        paymentMethod,
        paidAmountInput,
        discountRaw,
        customerId,
        customerRow,
        customerGlBalance,
        currentUserName,
        cartSubtotal,
        cartCost
    } = params;

    if (!cart.length) return { error: 'السلة فارغة' };

    const subtotal = cartSubtotal;
    const discount = clampDiscount(subtotal, discountRaw);
    const total = Math.max(0, subtotal - discount);
    const paid = effectivePaidAmount(paymentMethod, total, paidAmountInput);
    const due = Math.max(0, total - paid);

    if ((paymentMethod === 'credit' || paymentMethod === 'mixed') && !customerId) {
        return { error: 'اختر عميلاً للبيع الآجل أو المختلط' };
    }
    if (customerRow && (paymentMethod === 'credit' || paymentMethod === 'mixed')) {
        const base =
            customerGlBalance != null && !Number.isNaN(Number(customerGlBalance))
                ? Number(customerGlBalance)
                : (Number(customerRow.balance) || 0);
        const newBal = base + due;
        if (customerRow.credit_limit > 0 && newBal > customerRow.credit_limit) {
            return { error: 'تجاوز الحد الائتماني للعميل' };
        }
    }

    const updatedProducts = new Map();
    for (const item of cart) {
        const product = productsById.get(item.product_id);
        if (!product) return { error: 'منتج غير موجود' };
        const mult = getUnitMultiplier(item.unit);
        const required = item.quantity * mult;
        const stock = product.stock;
        if (invoiceType === 'sale' && stock < required) {
            return { error: `لا يوجد مخزون كافٍ للمنتج ${product.name} (المتاح: ${stock} حبة)` };
        }
        if (invoiceType === 'return' && stock + required > Number.MAX_SAFE_INTEGER) {
            return { error: 'خطأ في المخزون' };
        }
        const next = { ...product };
        if (invoiceType === 'sale') next.stock = stock - required;
        else next.stock = stock + required;
        if (next.stock < 0) return { error: `مخزون سالب غير مسموح: ${product.name}` };
        updatedProducts.set(product.id, next);
    }

    const now = new Date().toISOString();
    const invoice = {
        invoice_number: invoiceNumber,
        type: invoiceType,
        items: cart.map((item) => ({
            product_id: item.product_id,
            product_name: item.product_name,
            unit: item.unit,
            unit_multiplier: getUnitMultiplier(item.unit),
            price: item.price,
            quantity: item.quantity,
            line_subtotal: lineRevenue(item.price, item.quantity),
            base_pieces: item.quantity * getUnitMultiplier(item.unit)
        })),
        subtotal,
        discount,
        total,
        paid_amount: paid,
        due_amount: due,
        cost: cartCost,
        payment_method: paymentMethod,
        customer_id: customerId || null,
        customer_name: customerRow?.name || params.customerNameText || '',
        status: 'completed',
        created_by: currentUserName,
        created_at: now
    };

    const ops = [];
    for (const p of updatedProducts.values()) {
        ops.push({ type: 'put', store: 'products', value: p });
    }
    ops.push({ type: 'add', store: 'invoices', value: invoice });

    const journalId = newJournalId();
    const journalRows =
        invoiceType === 'sale'
            ? buildSaleJournal(journalId, {
                invoiceNumber,
                subtotal,
                discount,
                paid,
                due,
                cost: cartCost,
                customerId,
                now
            })
            : buildReturnJournal(journalId, {
                invoiceNumber,
                subtotal,
                discount,
                paid,
                due,
                cost: cartCost,
                customerId,
                now
            });

    for (const row of journalRows) {
        ops.push({ type: 'add', store: 'account_entries', value: row });
    }

    return { ops, invoice };
}

export function computeCartTotals(cart) {
    let subtotal = 0;
    let cost = 0;
    for (const item of cart) {
        subtotal += lineRevenue(item.price, item.quantity);
        cost += lineCost(item.cost, item.quantity, item.unit);
    }
    return { subtotal, cost };
}

export function getPriceByUnit(product, selectedUnit) {
    switch (selectedUnit) {
        case 'box':
            return product.box_price || 0;
        case 'pack':
            return product.wholesale_price || (product.retail_price * CONFIG.UNIT_CONVERSION.pack);
        default:
            return product.retail_price;
    }
}
