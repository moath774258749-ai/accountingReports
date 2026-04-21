import { CONFIG } from './constants.js';

/** Pieces per selling unit (piece=1, pack=10, box=500 with default CONFIG). */
export function getUnitMultiplier(unit) {
    const m = {
        piece: 1,
        pack: CONFIG.UNIT_CONVERSION.pack,
        box: CONFIG.UNIT_CONVERSION.box * CONFIG.UNIT_CONVERSION.pack
    };
    return m[unit] ?? 1;
}

/** Base pieces sold for one line: quantity × multiplier */
export function lineBasePieces(quantity, unit) {
    return quantity * getUnitMultiplier(unit);
}

export function getUnitName(unit) {
    switch (unit) {
        case 'box': return 'كرتون';
        case 'pack': return 'عروسة';
        default: return 'حبة';
    }
}

/** Revenue for line: unit price is per selected unit → × quantity only (no extra multiplier). */
export function lineRevenue(unitPrice, quantity) {
    return unitPrice * quantity;
}

/** COGS for line: cost per base piece × base pieces */
export function lineCost(costPerPiece, quantity, unit) {
    return costPerPiece * lineBasePieces(quantity, unit);
}

export function convertStock(pieces) {
    const perBox = CONFIG.UNIT_CONVERSION.box * CONFIG.UNIT_CONVERSION.pack;
    const boxes = Math.floor(pieces / perBox);
    const remaining = pieces % perBox;
    const packs = Math.floor(remaining / CONFIG.UNIT_CONVERSION.pack);
    const finalPieces = remaining % CONFIG.UNIT_CONVERSION.pack;
    return { boxes, packs, pieces: finalPieces };
}
