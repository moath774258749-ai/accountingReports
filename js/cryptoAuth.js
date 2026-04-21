const PBKDF2_ITER = 120000;
const PBKDF2_PREFIX = 'pbkdf2$';

/** Legacy hash for migrating existing DB users */
export function hashPasswordLegacy(password) {
    let hash = 0;
    for (let i = 0; i < password.length; i++) {
        const char = password.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash = hash & hash;
    }
    return hash.toString(16);
}

function toHex(buf) {
    return [...new Uint8Array(buf)].map(b => b.toString(16).padStart(2, '0')).join('');
}

function fromHex(hex) {
    const arr = new Uint8Array(hex.length / 2);
    for (let i = 0; i < arr.length; i++) {
        arr[i] = parseInt(hex.substr(i * 2, 2), 16);
    }
    return arr;
}

export async function hashPassword(password) {
    const enc = new TextEncoder();
    const salt = crypto.getRandomValues(new Uint8Array(16));
    const keyMaterial = await crypto.subtle.importKey(
        'raw',
        enc.encode(password),
        { name: 'PBKDF2' },
        false,
        ['deriveBits']
    );
    const bits = await crypto.subtle.deriveBits(
        {
            name: 'PBKDF2',
            salt,
            iterations: PBKDF2_ITER,
            hash: 'SHA-256'
        },
        keyMaterial,
        256
    );
    return `${PBKDF2_PREFIX}${PBKDF2_ITER}$${toHex(salt)}$${toHex(bits)}`;
}

export async function verifyPassword(password, stored) {
    if (!stored) return false;
    if (!stored.startsWith(PBKDF2_PREFIX)) {
        return hashPasswordLegacy(password) === stored;
    }
    const parts = stored.split('$');
    if (parts.length !== 4) return false;
    const iterations = parseInt(parts[1], 10);
    const salt = fromHex(parts[2]);
    const expected = parts[3];
    const enc = new TextEncoder();
    const keyMaterial = await crypto.subtle.importKey(
        'raw',
        enc.encode(password),
        { name: 'PBKDF2' },
        false,
        ['deriveBits']
    );
    const bits = await crypto.subtle.deriveBits(
        { name: 'PBKDF2', salt, iterations, hash: 'SHA-256' },
        keyMaterial,
        256
    );
    return toHex(bits) === expected;
}

export function createSessionPayload(user) {
    const issued = Date.now();
    return {
        v: 1,
        issued,
        user: {
            id: user.id,
            username: user.username || '',
            role: user.role,
            name: user.name || ''
        }
    };
}

export function readSession(ttlMs) {
    const raw = sessionStorage.getItem('current_session');
    if (!raw) return null;
    try {
        const data = JSON.parse(raw);
        if (!data || data.v !== 1 || !data.user || !data.issued) return null;
        if (Date.now() - data.issued > ttlMs) {
            sessionStorage.removeItem('current_session');
            return null;
        }
        return data.user;
    } catch {
        return null;
    }
}

export function writeSession(payload) {
    sessionStorage.setItem('current_session', JSON.stringify(payload));
}

export function clearSession() {
    sessionStorage.removeItem('current_session');
    try {
        sessionStorage.removeItem('current_user');
    } catch { /* legacy */ }
}
