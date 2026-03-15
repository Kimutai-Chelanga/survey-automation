// modules/chrome/ProfileManager.js
import { promises as fsp } from 'fs';
import path from 'path';

/**
 * Manages Chrome profile loading and validation.
 *
 * PERMISSION FIX:
 *   - Profile directories are created with mode 0o777 if they don't exist
 *   - chmod 0o777 is attempted on every profile dir at load time
 *   - Recordings and downloads dirs are also ensured writable
 */
export class ProfileManager {
    constructor(mongoDBService, config) {
        this.mongoDBService = mongoDBService;
        this.config = config;
        this.profiles = new Map();
    }

    /**
     * Ensure a directory exists and is writable by the current process.
     * Creates it (recursively) if absent, then attempts chmod 0o777.
     */
    async ensureWritableDir(dirPath) {
        try {
            await fsp.mkdir(dirPath, { recursive: true, mode: 0o777 });
        } catch (err) {
            if (err.code !== 'EEXIST') {
                console.warn(`  ⚠ Could not create directory ${dirPath}: ${err.message}`);
                return;
            }
        }
        try {
            await fsp.chmod(dirPath, 0o777);
        } catch (err) {
            // chmod may fail if owned by a different user but the dir is already
            // world-writable from Docker init — silently continue.
            console.warn(`  ⚠ chmod 777 failed for ${dirPath}: ${err.message} (may be fine)`);
        }
    }

    /**
     * Load all Streamlit-created Chrome profiles from MongoDB.
     * Also ensures the recordings and downloads directories are writable.
     */
    async loadProfiles() {
        console.log('\n' + '='.repeat(80));
        console.log('LOADING STREAMLIT-CREATED PROFILES');
        console.log('='.repeat(80) + '\n');

        // Ensure shared writable directories exist
        await this.ensureWritableDir(this.config.baseProfileDir);
        await this.ensureWritableDir(this.config.recordingsDir);
        await this.ensureWritableDir(this.config.downloadsDir);

        const profiles = await this.mongoDBService.db.collection('accounts').find({
            profile_type: 'local_chrome',
            is_active: { $ne: false }
        }).toArray();

        if (!profiles || profiles.length === 0) {
            console.log('⚠ No local Chrome profiles found in MongoDB');
            return;
        }

        for (const profile of profiles) {
            const accountId  = profile.postgres_account_id;
            const username   = profile.username;
            const profilePath = profile.profile_path || this.config.getProfilePath(username);

            // Create profile directory if it doesn't exist yet
            await this.ensureWritableDir(profilePath);

            // Also ensure the Default sub-directory exists (Chrome expects it)
            await this.ensureWritableDir(path.join(profilePath, 'Default'));

            // Verify the directory is now accessible
            try {
                await fsp.access(profilePath, fsp.constants?.W_OK ?? 2); // 2 = W_OK
            } catch (err) {
                console.warn(`⚠ Profile directory not writable after chmod: ${profilePath}`);
                console.warn(`  Error: ${err.message}`);
                console.warn(`  Skipping account ${username}`);
                continue;
            }

            this.profiles.set(accountId, {
                username:    username,
                profilePath: profilePath,
                profileId:   profile.profile_id,
                mongoId:     profile._id.toString(),
                createdVia:  'streamlit'
            });

            console.log(`✓ Loaded profile: ${username}`);
            console.log(`  Path:       ${profilePath}`);
            console.log(`  Account ID: ${accountId}`);
        }

        console.log(`\n✓ Loaded ${this.profiles.size} Streamlit profiles\n`);
    }

    /**
     * Get profile by account ID.
     */
    getProfile(accountId) {
        const profileInfo = this.profiles.get(accountId);

        if (!profileInfo) {
            throw new Error(
                `No profile found for account ${accountId}. ` +
                `Available accounts: [${Array.from(this.profiles.keys()).join(', ')}]`
            );
        }

        return {
            accountId:   accountId,
            username:    profileInfo.username,
            profileId:   profileInfo.profileId,
            profilePath: profileInfo.profilePath,
            profileType: 'local_chrome',
            preStarted:  false
        };
    }

    /**
     * Get all loaded profiles.
     */
    getAllProfiles() {
        return Array.from(this.profiles.values());
    }
}
