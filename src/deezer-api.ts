import fetch from 'node-fetch';

/**
 * Deezer API client with multi-key rotation for RapidAPI.
 * Supports up to 10 API keys loaded from environment variables.
 */

const RAPIDAPI_HOST = 'deezerdevs-deezer.p.rapidapi.com';
const BASE_URL = `https://${RAPIDAPI_HOST}`;

// Load all available API keys from environment
const RAPID_API_KEYS: string[] = [];
for (let i = 1; i <= 10; i++) {
    const key = process.env[`RAPIDAPI_KEY_${i}`];
    if (key && key !== 'your_key_here') {
        RAPID_API_KEYS.push(key);
    }
}

if (RAPID_API_KEYS.length === 0) {
    console.error('❌ No RAPIDAPI_KEY_* environment variables found. Set at least RAPIDAPI_KEY_1.');
    process.exit(1);
}

let currentKeyIndex = 0;
let totalApiCalls = 0;
let failedApiCalls = 0;

function getNextKey(): string {
    const key = RAPID_API_KEYS[currentKeyIndex];
    currentKeyIndex = (currentKeyIndex + 1) % RAPID_API_KEYS.length;
    return key;
}

export function getApiStats() {
    return {
        totalKeys: RAPID_API_KEYS.length,
        totalApiCalls,
        failedApiCalls,
        successRate: totalApiCalls > 0 
            ? Math.round(((totalApiCalls - failedApiCalls) / totalApiCalls) * 100) 
            : 0
    };
}

// Rate limiter: minimum delay between calls (per key)
const SLEEP_MS = parseInt(process.env.SLEEP_MS || '200');
export const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

interface DeezerArtistResponse {
    id: number;
    name: string;
    link: string;
    share: string;
    picture: string;
    picture_small: string;
    picture_medium: string;
    picture_big: string;
    picture_xl: string;
    nb_album: number;
    nb_fan: number;
    radio: boolean;
    tracklist: string;
    type: string;
    error?: { type: string; message: string; code: number };
}

interface DeezerAlbumResponse {
    id: number;
    title: string;
    link: string;
    share: string;
    cover: string;
    cover_small: string;
    cover_medium: string;
    cover_big: string;
    cover_xl: string;
    nb_tracks: number;
    duration: number;
    fans: number;
    release_date: string;
    record_type: string;
    genre_id: number;
    label: string;
    tracklist: string;
    type: string;
    artist: {
        id: number;
        name: string;
        picture_xl: string;
    };
    genres?: {
        data: Array<{ id: number; name: string; picture: string }>;
    };
    tracks?: {
        data: Array<{
            id: number;
            title: string;
            duration: number;
            rank: number;
            preview: string;
        }>;
    };
    error?: { type: string; message: string; code: number };
}

/**
 * Fetch artist data from Deezer API.
 * @param artistId The Deezer artist ID (numeric string)
 */
export async function fetchDeezerArtist(artistId: string): Promise<DeezerArtistResponse | null> {
    const key = getNextKey();
    totalApiCalls++;

    try {
        const response = await fetch(`${BASE_URL}/artist/${artistId}`, {
            method: 'GET',
            headers: {
                'x-rapidapi-host': RAPIDAPI_HOST,
                'x-rapidapi-key': key
            }
        });

        if (!response.ok) {
            failedApiCalls++;
            console.error(`   ❌ API Error for artist/${artistId}: ${response.status} ${response.statusText}`);
            return null;
        }

        const data = await response.json() as DeezerArtistResponse;

        if (data.error) {
            failedApiCalls++;
            console.error(`   ⚠️ Deezer Error for artist/${artistId}: ${data.error.message}`);
            return null;
        }

        return data;
    } catch (err: any) {
        failedApiCalls++;
        console.error(`   ❌ Network error for artist/${artistId}:`, err.message);
        return null;
    }
}

/**
 * Fetch album data from Deezer API.
 * @param albumId The Deezer album ID (numeric string)
 */
export async function fetchDeezerAlbum(albumId: string): Promise<DeezerAlbumResponse | null> {
    const key = getNextKey();
    totalApiCalls++;

    try {
        const response = await fetch(`${BASE_URL}/album/${albumId}`, {
            method: 'GET',
            headers: {
                'x-rapidapi-host': RAPIDAPI_HOST,
                'x-rapidapi-key': key
            }
        });

        if (!response.ok) {
            failedApiCalls++;
            console.error(`   ❌ API Error for album/${albumId}: ${response.status} ${response.statusText}`);
            return null;
        }

        const data = await response.json() as DeezerAlbumResponse;

        if (data.error) {
            failedApiCalls++;
            console.error(`   ⚠️ Deezer Error for album/${albumId}: ${data.error.message}`);
            return null;
        }

        return data;
    } catch (err: any) {
        failedApiCalls++;
        console.error(`   ❌ Network error for album/${albumId}:`, err.message);
        return null;
    }
}

export { SLEEP_MS };
