// modules/recording/VideoRecorder.js
import { spawn } from 'child_process';
import { promises as fsp } from 'fs';
import path from 'path';

/**
 * Handles FFmpeg video recording.
 *
 * FIXES APPLIED:
 *   - Added `-pix_fmt yuv420p`       → broad player compatibility (Windows Media Player, browsers)
 *   - Added `-movflags +faststart`    → moov atom moved to front for browser streaming / seeking
 *   - Added `-vf scale=1920:1080`     → ensures even dimensions (required by libx264)
 *   - Permissions: recordingsDir is created with mode 0o777 before every recording attempt
 *   - chmod 0o777 applied to the output file after creation
 */
export class VideoRecorder {
    constructor(recordingsDir) {
        // Use /workspace/recordings — always writable in Docker setup
        this.recordingsDir = recordingsDir || process.env.RECORDINGS_DIR || '/workspace/recordings';
    }

    /**
     * Ensure the recordings directory exists and is writable.
     */
    async ensureRecordingsDirWritable() {
        try {
            await fsp.mkdir(this.recordingsDir, { recursive: true, mode: 0o777 });
        } catch (err) {
            if (err.code !== 'EEXIST') {
                console.warn(`  ⚠ Could not create recordings dir ${this.recordingsDir}: ${err.message}`);
                return;
            }
        }
        try {
            await fsp.chmod(this.recordingsDir, 0o777);
        } catch (err) {
            console.warn(`  ⚠ chmod 777 on recordings dir: ${err.message} (may be fine)`);
        }
    }

    /**
     * Start video recording.
     *
     * Key FFmpeg flags for compatibility:
     *   -pix_fmt yuv420p      → required by most players and browsers (Windows Media Player,
     *                            Chrome, Firefox). Without this, recordings may fail to open
     *                            with error 0x80004005 or similar codec errors.
     *   -movflags +faststart  → moves the moov atom to the front of the file so browsers
     *                            can start playback before the entire file is downloaded,
     *                            and Streamlit st.video() can stream without loading the
     *                            whole file into RAM first.
     *   -vf scale=1920:1080   → libx264 requires even dimensions; this guarantees it.
     */
    async startRecording(displayNum, sessionId, workflowInfo) {
        // Ensure dir is writable before starting ffmpeg
        await this.ensureRecordingsDirWritable();

        const timestamp  = new Date().toISOString().replace(/[:.]/g, '-');
        const filename   = `${sessionId}_${timestamp}.mp4`;
        const outputPath = path.join(this.recordingsDir, filename);

        console.log(`Starting video recording for display :${displayNum}`);
        console.log(`  Output: ${outputPath}`);

        try {
            const ffmpegArgs = [
                // Input
                '-f',           'x11grab',
                '-video_size',  '1920x1080',
                '-framerate',   '30',
                '-i',           `:${displayNum}`,

                // Codec
                '-c:v',         'libx264',
                '-preset',      'ultrafast',
                '-crf',         '23',

                // ── Compatibility fixes ─────────────────────────────────────
                // Required for Windows Media Player, all browsers, and Streamlit
                // streaming. Without yuv420p many decoders refuse to open the file.
                '-pix_fmt',     'yuv420p',

                // Ensure even dimensions — libx264 hard requirement.
                // 1920x1080 is already even but this guards against any
                // source resolution weirdness.
                '-vf',          'scale=trunc(iw/2)*2:trunc(ih/2)*2',

                // Move moov atom to front of file so browsers can seek/stream
                // without downloading the whole file first.
                // This is also what makes Streamlit st.video() not hang.
                '-movflags',    '+faststart',

                '-y',
                outputPath
            ];

            const ffmpegProcess = spawn('ffmpeg', ffmpegArgs, {
                stdio: ['ignore', 'pipe', 'pipe']
            });

            ffmpegProcess.stderr.on('data', (data) => {
                // Only log actual errors, not progress info
                const msg = data.toString();
                if (msg.includes('Error') || msg.includes('error')) {
                    console.error(`[FFmpeg] ${msg.trim()}`);
                }
            });

            ffmpegProcess.on('error', (error) => {
                console.error(`FFmpeg process error: ${error.message}`);
            });

            await this.sleep(2000);
            console.log('✓ Video recording started');

            return {
                process:    ffmpegProcess,
                outputPath,
                filename,
                startTime:  new Date(),
                displayNum
            };

        } catch (error) {
            console.error(`Failed to start video recording: ${error.message}`);
            return null;
        }
    }

    /**
     * Stop video recording.
     */
    async stopRecording(recordingInfo) {
        if (!recordingInfo || !recordingInfo.process) {
            console.log('No recording to stop');
            return null;
        }

        console.log('Stopping video recording...');

        try {
            recordingInfo.process.kill('SIGINT');

            await new Promise((resolve) => {
                recordingInfo.process.on('close', resolve);
                setTimeout(resolve, 5000);
            });

            console.log('✓ Video recording stopped');

            // Verify file exists and has content
            try {
                const stats = await fsp.stat(recordingInfo.outputPath);
                if (stats.size === 0) {
                    console.warn('⚠ Recording file is empty');
                    return null;
                }
                console.log(`  File size: ${(stats.size / 1024 / 1024).toFixed(2)} MB`);

                // Ensure the file is readable by subsequent steps
                await fsp.chmod(recordingInfo.outputPath, 0o666).catch(() => {});
            } catch (error) {
                console.error(`Recording file not found: ${error.message}`);
                return null;
            }

            return {
                outputPath: recordingInfo.outputPath,
                filename:   recordingInfo.filename,
                duration:   (new Date() - recordingInfo.startTime) / 1000,
                success:    true
            };

        } catch (error) {
            console.error(`Error stopping video recording: ${error.message}`);
            return null;
        }
    }

    /**
     * Upload recording to GridFS and clean up local file.
     */
    async uploadToGridFS(recordingInfo, workflowInfo, mongoDBService) {
        try {
            console.log('Uploading recording to GridFS...');

            const fileBuffer = await fsp.readFile(recordingInfo.outputPath);

            const gridfsFileId = await mongoDBService.uploadToGridFS(
                fileBuffer,
                recordingInfo.filename,
                {
                    contentType: 'video/mp4',
                    metadata: {
                        session_id:           workflowInfo.executionId,
                        session_type:         'local_chrome',
                        postgres_account_id:  workflowInfo.accountId,
                        account_username:     workflowInfo.username,
                        workflow_type:        workflowInfo.workflowType,
                        workflow_name:        workflowInfo.workflowName,
                        link_id:              workflowInfo.linkId,
                        execution_id:         workflowInfo.executionId,
                        duration_seconds:     recordingInfo.duration,
                        recorded_at:          new Date(),
                        uploaded_at:          new Date()
                    }
                },
                'video_recordings'
            );

            console.log(`✓ Recording uploaded to GridFS: ${gridfsFileId}`);

            // Clean up local file
            try {
                await fsp.unlink(recordingInfo.outputPath);
                console.log('✓ Local recording file cleaned up');
            } catch (error) {
                console.warn(`Could not delete local file: ${error.message}`);
            }

            return { ...recordingInfo, gridfs_file_id: gridfsFileId };

        } catch (error) {
            console.error(`Failed to upload recording to GridFS: ${error.message}`);
            return null;
        }
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
