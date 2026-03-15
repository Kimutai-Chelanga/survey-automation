Good question — yes, you can check them, but there’s an important distinction:

👉 Those RUN commands affect the image at build time
👉 They do NOT control permissions of named Docker volumes at runtime

Since you are using named volumes:

volumes:
  - chrome_profiles:/workspace/chrome_profiles
  - recordings:/workspace/recordings


Docker replaces whatever was created in the image with the volume mount.

So your Dockerfile permissions get overridden.

✅ 1️⃣ How To Check Current Permissions (Runtime)

Check inside the running container:

docker exec -it airflow_scheduler bash
ls -ld /workspace
ls -ld /workspace/chrome_profiles
ls -ld /workspace/recordings
exit


That shows what actually matters.

✅ 2️⃣ How To Fix Them Properly (Runtime Safe Way)

If they are wrong, fix them as root inside the container:

docker exec -u 0 -it airflow_scheduler bash
chown -R 50000:0 /workspace/chrome_profiles
chown -R 50000:0 /workspace/recordings
chmod -R 775 /workspace
exit


That is the correct runtime fix.

🔥 Important Concept

This part in your Dockerfile:

RUN mkdir -p /workspace/chrome_profiles /workspace/recordings \
    && chown -R airflow:root /workspace \
    && chmod -R 775 /workspace


Only applies if:

You are NOT mounting named volumes

Or you are using bind mounts to host folders

Since you're using named volumes, Docker does this:

Container starts

Volume mounts over /workspace/chrome_profiles

Original image permissions are hidden

Volume permissions take over

So changing Dockerfile won’t fix volume ownership.

🎯 Best Permanent Solution (You Already Have It)

Your volume-init container should contain:

chown -R 50000:0 /workspace/chrome_profiles
chown -R 50000:0 /workspace/recordings
chmod -R 775 /workspace


That guarantees correct ownership every time.

🧠 Quick Rule To Remember
Location	Controlled By
Dockerfile RUN	Image build time
Named volumes	Runtime container user
bind mounts	Host filesystem
🚀 If You Want To Verify Everything Is Perfect

After fixing:

docker exec -it airflow_scheduler bash
ls -ld /workspace
ls -ld /workspace/chrome_profiles
ls -ld /workspace/recordings
id


You want:

50000 0
drwxrwxr-x


Then you are 100% safe from permission issues.

If you'd like, I can give you a short “Docker Volume Permission Rules” section formatted for your README as well.

93c4:/opt/airflow$ ls -ld /workspace
ls -ld /workspace/chrome_profiles
ls -ld /workspace/recordings
drwxr-xr-x 5 airflow root 4096 Feb 14 16:21 /workspace
drwxrwxrwx 4 airflow root 4096 Feb 14 16:25 /workspace/chrome_profiles
drwxrwxrwx 2 airflow root 4096 Feb 14 16:21 /workspace/recordings
airflow@90d34d7c93c4:/opt/airflow$ 

Perfect — this confirms your volumes are correctly owned and writable by the airflow user:

drwxr-xr-x 5 airflow root      /workspace
drwxrwxrwx 4 airflow root      /workspace/chrome_profiles
drwxrwxrwx 2 airflow root      /workspace/recordings


✅ Interpretation:

airflow is UID 50000 → has ownership

root is GID 0 → group

chrome_profiles and recordings are 777 → fully writable

/workspace itself is 755 → readable/executable by all, writable only by owner (airflow)

This is exactly what you want for Chrome profiles, recordings, and other workspace files.

No changes are needed right now. Any Chrome issues related to permissions should now be resolved.

If you want, I can give you a one-liner README snippet that shows exactly how to verify these permissions in the future. Do you want me to do that?
