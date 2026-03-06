import { supabase } from './supabase';
import { fetchDeezerAlbum, getApiStats, sleep, SLEEP_MS } from './deezer-api';

/**
 * Deezer Media Profile Enrichment
 * 
 * Fetches album/single data from the Deezer API and updates media_profiles records.
 * Uses the deezer_url column to extract the Deezer album ID.
 * 
 * Processing order:
 *   1. Records where last_processed IS NULL (never processed) — first  [NOTE: using deezer_check]
 *   2. Records where deezer_check IS NOT NULL — ordered by oldest first
 * 
 * Environment variables:
 *   - LIMIT: Max records to process per run (default: 1000)
 *   - SUPABASE_URL, SUPABASE_SERVICE_KEY: Supabase credentials
 *   - RAPIDAPI_KEY_1..RAPIDAPI_KEY_10: RapidAPI keys for rotation
 */

const LIMIT_ENV = process.env.LIMIT;
const RUN_ALL = !LIMIT_ENV || LIMIT_ENV.trim() === '';
const LIMIT = RUN_ALL ? 999999 : parseInt(LIMIT_ENV as string);
const WORKFLOW_NAME = 'Deezer Media Enrichment';

/**
 * Extract the Deezer album ID from a URL like:
 *   https://www.deezer.com/album/620915621
 *   https://deezer.com/album/620915621
 */
function extractAlbumId(url: string): string | null {
    const match = url.match(/deezer\.com\/album\/(\d+)/);
    return match ? match[1] : null;
}

async function updateWorkflowLog(phase: string, message: string, stats?: Record<string, any>) {
    try {
        const logEntry = {
            timestamp: new Date().toISOString(),
            phase,
            message,
            ...(stats || {})
        };

        const { data: workflow } = await supabase
            .from('workflows')
            .select('id, workflow_logs')
            .eq('name', WORKFLOW_NAME)
            .single();

        if (workflow) {
            const existingLogs = (workflow.workflow_logs as any[]) || [];
            const updatedLogs = [...existingLogs.slice(-99), logEntry];

            await supabase
                .from('workflows')
                .update({
                    workflow_logs: updatedLogs,
                    updated_at: new Date().toISOString()
                })
                .eq('id', workflow.id);
        }
    } catch (err) {
        console.warn('   ⚠️ Could not update workflow log:', err);
    }
}

async function enrichDeezerMediaProfiles() {
    const startTime = Date.now();
    console.log('🚀 Starting Deezer Media Profile Enrichment...');
    console.log(`📦 Limit: ${LIMIT} records`);

    const apiStats = getApiStats();
    console.log(`📡 Using ${apiStats.totalKeys} API keys in rotation.`);

    await updateWorkflowLog('start', `Starting media enrichment run with limit=${LIMIT}`, { keys: apiStats.totalKeys });

    // ─── Fetch records: unprocessed first, then oldest processed ────────────
    // Step 1: Get records with deezer_url but no deezer_check (never processed)
    const { data: unprocessed, error: err1 } = await supabase
        .from('media_profiles')
        .select('id, album_name, artist_name, deezer_url, deezer_check')
        .not('deezer_url', 'is', null)
        .neq('deezer_url', '')
        .is('deezer_check', null)
        .order('created_at', { ascending: true })
        .limit(LIMIT);

    if (err1) {
        console.error('❌ Error fetching unprocessed media profiles:', err1.message);
        await updateWorkflowLog('error', `Fetch error: ${err1.message}`);
        return;
    }

    let profiles = unprocessed || [];
    const remainingSlots = LIMIT - profiles.length;

    // Step 2: If we still have room, get oldest-checked records
    if (remainingSlots > 0) {
        const { data: oldProcessed, error: err2 } = await supabase
            .from('media_profiles')
            .select('id, album_name, artist_name, deezer_url, deezer_check')
            .not('deezer_url', 'is', null)
            .neq('deezer_url', '')
            .not('deezer_check', 'is', null)
            .order('updated_at', { ascending: true })
            .limit(remainingSlots);

        if (!err2 && oldProcessed) {
            profiles = [...profiles, ...oldProcessed];
        }
    }

    if (profiles.length === 0) {
        console.log('✅ No Deezer media profiles to process.');
        await updateWorkflowLog('complete', 'No records to process');
        return;
    }

    console.log(`📋 Processing ${profiles.length} media profiles (${(unprocessed || []).length} new, ${profiles.length - (unprocessed || []).length} re-check)...\n`);

    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;

    for (let i = 0; i < profiles.length; i++) {
        const profile = profiles[i];
        const progress = `[${i + 1}/${profiles.length}]`;

        const albumId = extractAlbumId(profile.deezer_url);
        if (!albumId) {
            console.log(`${progress} ⏭️  Skipping: ${profile.album_name} — cannot extract album ID from URL: ${profile.deezer_url}`);
            skippedCount++;
            continue;
        }

        console.log(`${progress} Processing: ${profile.artist_name} — ${profile.album_name} (album/${albumId})`);

        const albumData = await fetchDeezerAlbum(albumId);

        if (albumData) {
            const now = new Date().toISOString();

            const updatePayload: Record<string, any> = {
                deezer_id: albumData.id.toString(),
                deezer_type: albumData.record_type,
                deezer_fans: albumData.fans,
                deezer_genre_id: albumData.genre_id,
                deezer_check: 'done',
                // Update shared fields if they are empty
                ...(profile.album_name ? {} : { album_name: albumData.title }),
                cover_art_url: albumData.cover_xl || undefined,
                track_count: albumData.nb_tracks?.toString(),
                release_date: albumData.release_date || undefined,
                label: albumData.label || undefined,
                updated_at: now
            };

            // Remove undefined values
            Object.keys(updatePayload).forEach(key => {
                if (updatePayload[key] === undefined) delete updatePayload[key];
            });

            const { error: updateError } = await supabase
                .from('media_profiles')
                .update(updatePayload)
                .eq('id', profile.id);

            if (updateError) {
                console.error(`   ❌ Update error for ${profile.id}:`, updateError.message);
                errorCount++;
            } else {
                console.log(`   ✅ ${albumData.title} — ${albumData.record_type}, ${albumData.nb_tracks} tracks, ${albumData.fans?.toLocaleString()} fans`);
                successCount++;
            }
        } else {
            // Mark as checked with error
            const now = new Date().toISOString();
            await supabase
                .from('media_profiles')
                .update({
                    deezer_check: 'error',
                    updated_at: now
                })
                .eq('id', profile.id);
            skippedCount++;
        }

        await sleep(SLEEP_MS);
    }

    // ─── Summary ────────────────────────────────────────────────────────────
    const elapsed = Math.round((Date.now() - startTime) / 1000);
    const finalStats = getApiStats();

    console.log('\n==========================================');
    console.log(`✨ Deezer Media Enrichment Complete!`);
    console.log(`   ✅ Success: ${successCount}`);
    console.log(`   ❌ Errors:  ${errorCount}`);
    console.log(`   ⏭️  Skipped: ${skippedCount}`);
    console.log(`   ⏱️  Time:    ${elapsed}s`);
    console.log(`   📡 API Calls: ${finalStats.totalApiCalls} (${finalStats.successRate}% success)`);
    console.log('==========================================\n');

    // Update the workflow record with final stats
    try {
        const { data: wf } = await supabase
            .from('workflows')
            .select('id')
            .eq('name', WORKFLOW_NAME)
            .single();

        if (wf) {
            const { count: todoCount } = await supabase
                .from('media_profiles')
                .select('*', { count: 'exact', head: true })
                .not('deezer_url', 'is', null)
                .neq('deezer_url', '')
                .is('deezer_check', null);

            const { count: doneCount } = await supabase
                .from('media_profiles')
                .select('*', { count: 'exact', head: true })
                .not('deezer_url', 'is', null)
                .neq('deezer_url', '')
                .eq('deezer_check', 'done');

            await supabase
                .from('workflows')
                .update({
                    to_process: todoCount || 0,
                    processed: doneCount || 0,
                    last_run_at: new Date().toISOString(),
                    status: 'active',
                    health_score: errorCount === 0 ? 100 : Math.max(50, 100 - (errorCount * 5)),
                    updated_at: new Date().toISOString()
                })
                .eq('id', wf.id);
        }
    } catch (err) {
        console.warn('⚠️ Could not update workflow stats:', err);
    }

    await updateWorkflowLog('complete', `Run finished: ${successCount} success, ${errorCount} errors, ${skippedCount} skipped`, {
        elapsed_seconds: elapsed,
        api_calls: finalStats.totalApiCalls,
        success_rate: `${finalStats.successRate}%`
    });
}

// ─── Entry point ─────────────────────────────────────────────────────────────
enrichDeezerMediaProfiles().catch(err => {
    console.error('💥 Fatal error:', err);
    process.exit(1);
});
