import { supabase } from './supabase';
import { fetchDeezerArtist, getApiStats, sleep, SLEEP_MS } from './deezer-api';

/**
 * Deezer Social Profile Enrichment
 *
 * Fetches artist data from the Deezer API and updates hb_socials records.
 *
 * Processing order:
 *   1. Records where check_deezer_enrichment IS NULL (never processed) — first
 *   2. Records where check_deezer_enrichment IS NOT NULL — ordered ASC (oldest first)
 *
 * Environment variables:
 *   - LIMIT: Max records to process per run (default: 1000)
 *   - SUPABASE_URL, SUPABASE_SERVICE_KEY: Supabase credentials
 *   - RAPIDAPI_KEY_1..RAPIDAPI_KEY_10: RapidAPI keys for rotation
 */

const LIMIT_ENV = process.env.LIMIT;
const RUN_ALL = !LIMIT_ENV || LIMIT_ENV.trim() === '';
const LIMIT = RUN_ALL ? 999999 : parseInt(LIMIT_ENV as string);
const WORKFLOW_NAME = 'Deezer Social Enrichment';

function cleanHandle(name: string): string {
    return name.toLowerCase().replace(/[^a-z0-9]/g, '');
}

async function updateWorkflowLog(phase: string, message: string, stats?: Record<string, any>) {
    try {
        const logEntry = {
            timestamp: new Date().toISOString(),
            phase,
            message,
            ...(stats || {})
        };

        // Find the workflow record
        const { data: workflow } = await supabase
            .from('workflows')
            .select('id, workflow_logs')
            .eq('name', WORKFLOW_NAME)
            .single();

        if (workflow) {
            const existingLogs = (workflow.workflow_logs as any[]) || [];
            // Keep last 100 log entries
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
        // Don't fail the whole run for a logging error
        console.warn('   ⚠️ Could not update workflow log:', err);
    }
}

async function enrichDeezerSocialProfiles() {
    const startTime = Date.now();
    console.log('🚀 Starting Deezer Social Profile Enrichment...');
    console.log(`📦 Limit: ${LIMIT} records`);

    const apiStats = getApiStats();
    console.log(`📡 Using ${apiStats.totalKeys} API keys in rotation.`);

    await updateWorkflowLog('start', `Starting enrichment run with limit=${LIMIT}`, { keys: apiStats.totalKeys });

    // ─── Fetch records: unprocessed first, then oldest processed ────────────
    // Step 1: Get unprocessed records (check_deezer_enrichment IS NULL)
    const { data: unprocessed, error: err1 } = await supabase
        .from('hb_socials')
        .select('id, identifier, name, check_deezer_enrichment')
        .eq('type', 'DEEZER')
        .not('identifier', 'is', null)
        .neq('identifier', '')
        .neq('identifier', 'not.found')
        .is('check_deezer_enrichment', null)
        .order('last_check', { ascending: true, nullsFirst: true })
        .limit(LIMIT);

    if (err1) {
        console.error('❌ Error fetching unprocessed profiles:', err1.message);
        await updateWorkflowLog('error', `Fetch error: ${err1.message}`);
        return;
    }

    let profiles = unprocessed || [];
    const remainingSlots = LIMIT - profiles.length;

    // Step 2: If we still have room, get oldest-processed records
    if (remainingSlots > 0) {
        const { data: oldProcessed, error: err2 } = await supabase
            .from('hb_socials')
            .select('id, identifier, name, check_deezer_enrichment')
            .eq('type', 'DEEZER')
            .not('identifier', 'is', null)
            .neq('identifier', '')
            .neq('identifier', 'not.found')
            .not('check_deezer_enrichment', 'is', null)
            .order('check_deezer_enrichment', { ascending: true })
            .limit(remainingSlots);

        if (!err2 && oldProcessed) {
            profiles = [...profiles, ...oldProcessed];
        }
    }

    if (profiles.length === 0) {
        console.log('✅ No Deezer profiles to process.');
        await updateWorkflowLog('complete', 'No records to process');
        return;
    }

    console.log(`📋 Processing ${profiles.length} profiles (${(unprocessed || []).length} new, ${profiles.length - (unprocessed || []).length} re-check)...\n`);

    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;

    for (let i = 0; i < profiles.length; i++) {
        const profile = profiles[i];
        const progress = `[${i + 1}/${profiles.length}]`;

        console.log(`${progress} Processing: ${profile.name || profile.identifier} (ID: ${profile.identifier})`);

        const deezerData = await fetchDeezerArtist(profile.identifier);

        if (deezerData) {
            const now = new Date().toISOString();

            // Build gallery_images JSONB object with all sizes
            const galleryImages = {
                picture: deezerData.picture,
                picture_small: deezerData.picture_small,
                picture_medium: deezerData.picture_medium,
                picture_big: deezerData.picture_big,
                picture_xl: deezerData.picture_xl
            };

            const updatePayload: Record<string, any> = {
                name: deezerData.name,
                handle: cleanHandle(deezerData.name),
                social_url: deezerData.link,
                image: deezerData.picture_xl,
                description: deezerData.share,
                followers: deezerData.nb_fan,
                media_count: deezerData.nb_album,
                gallery_images: galleryImages,
                verified: false,
                status: 'done',
                check_deezer_enrichment: now,
                last_check: now,
                updated_at: now,
                history_json: {
                    last_run: now,
                    source: 'deezer-social-enrichment',
                    deezer_id: deezerData.id,
                    nb_fan: deezerData.nb_fan,
                    nb_album: deezerData.nb_album
                }
            };

            const { error: updateError } = await supabase
                .from('hb_socials')
                .update(updatePayload)
                .eq('id', profile.id);

            if (updateError) {
                console.error(`   ❌ Update error for ${profile.id}:`, updateError.message);
                errorCount++;
            } else {
                console.log(`   ✅ ${deezerData.name} — ${deezerData.nb_fan?.toLocaleString()} fans, ${deezerData.nb_album} albums`);
                successCount++;
            }
        } else {
            // Mark as checked but with error status
            const now = new Date().toISOString();
            await supabase
                .from('hb_socials')
                .update({
                    check_deezer_enrichment: now,
                    last_check: now,
                    updated_at: now,
                    history_json: {
                        last_run: now,
                        source: 'deezer-social-enrichment',
                        status: 'api_error'
                    }
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
    console.log(`✨ Deezer Social Enrichment Complete!`);
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
            .select('id, to_process, processed')
            .eq('name', WORKFLOW_NAME)
            .single();

        if (wf) {
            // Get fresh counts
            const { count: totalCount } = await supabase
                .from('hb_socials')
                .select('*', { count: 'exact', head: true })
                .eq('type', 'DEEZER')
                .not('identifier', 'is', null)
                .neq('identifier', '')
                .neq('identifier', 'not.found')
                .is('check_deezer_enrichment', null);

            const { count: processedCount } = await supabase
                .from('hb_socials')
                .select('*', { count: 'exact', head: true })
                .eq('type', 'DEEZER')
                .not('identifier', 'is', null)
                .neq('identifier', '')
                .neq('identifier', 'not.found')
                .not('check_deezer_enrichment', 'is', null);

            await supabase
                .from('workflows')
                .update({
                    to_process: totalCount || 0,
                    processed: processedCount || 0,
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
enrichDeezerSocialProfiles().catch(err => {
    console.error('💥 Fatal error:', err);
    process.exit(1);
});
