# Hosting the Live Demo on Azure

How to run the live side of the project (Kafka, the live producer, the streaming job, and the dashboard) on one Azure VM with a stable HTTPS address. Batch jobs and training stay on the laptop.

The demo runs at https://vessel-watch.northcentralus.cloudapp.azure.com (first deployed Nov 12, 2026, on a Standard_B2s VM in North Central US). Steps 1 to 6 were run once for that deployment and corrected from what happened; step 7 (memory check) is still open.

Throughout, replace `HOST` with the VM's DNS name, `LABEL.REGION.cloudapp.azure.com`, and `USER` in `ssh` and `rsync` commands with your login name on the VM.

## 1. Create the VM (Azure portal)

Virtual machines, then Create, with:

| Setting | Value |
| :-- | :-- |
| Region | One of the regions your subscription allows (see below) |
| Image | Ubuntu Server 24.04 LTS, x64 |
| Size | Standard_B2s (2 vCPU, 4 GB). If it is unavailable, B2als_v2 or B2s_v2 |
| Authentication | SSH public key |
| Inbound ports | 22, 80, 443 |
| OS disk | Default 30 GB |

Do not open ports 8501 (Streamlit) or 9092 (Kafka). The dashboard is reached through the web server on 443, and Kafka is only used on the machine itself.

**Regions.** An Azure for Students subscription can only create resources in a few regions. A wrong region fails with `RequestDisallowedByAzure`, whatever the image or size. To see your list: Policy, then Assignments, open the assignment about allowed regions, and look at its Parameters tab.

**Stable address.** After the VM is created, open its Public IP resource, then Configuration. Set the assignment to Static and enter a DNS name label. The address is then `LABEL.REGION.cloudapp.azure.com`.

## 2. Install the tools (on the server)

```bash
ssh USER@HOST

# 2 GB swap as a safety net on the 4 GB machine; skip if `swapon --show` already lists a swap file
swapon --show
sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
sudo mkswap /swapfile && sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

sudo apt update
sudo apt install -y git make openjdk-17-jre-headless caddy
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker "$USER"
curl -LsSf https://astral.sh/uv/install.sh | sh
exit
```

Log out and back in so the Docker group and `uv` take effect.

## 3. Get the code (on the server)

```bash
ssh USER@HOST
git clone https://github.com/engindenizdogu/vessel-watch.git
cd vessel-watch
make env
echo 'AISSTREAM_API_KEY=your-key-here' > .env && chmod 600 .env
```

To deploy a branch that is not merged yet, add `-b BRANCH` to the clone. `make env` downloads Python 3.11 through `uv` if the server does not have it.

## 4. Copy the data (from the laptop)

The History page and the map layers read the cleaned NOAA days and the learned tables, which are not in git (about 260 MB). Cloud Shell cannot see the laptop's files, so the laptop needs its own SSH access first.

**a. Give the laptop access (once).** On the laptop, print the public key (create one with `ssh-keygen -t ed25519` if the file is missing):

```bash
cat ~/.ssh/id_ed25519.pub
```

On the server, add that one line to your user's allowed keys:

```bash
mkdir -p ~/.ssh && chmod 700 ~/.ssh
echo 'PASTE_KEY_HERE' >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

Back on the laptop, test it. Use `-l` for the login name, because a school login such as `name@school.edu` contains an `@`:

```bash
ssh -l 'USER' HOST
```

Not confirmed yet: an account that signs in with a school login may ignore added keys. If the test is refused, use the admin user and key set when the VM was created instead (`ssh -i KEY.pem ADMIN@HOST`).

**b. Copy the data.** In the repo folder on the laptop:

```bash
rsync -av -e "ssh -l 'USER'" \
  --exclude raw --exclude checkpoints --exclude serving --exclude live --exclude live_sample \
  data/ HOST:vessel-watch/data/
```

The laptop's live history (`data/live/`) is left out; the server collects its own from the day it starts.

## 5. Run everything as services (on the server)

Four systemd services start on boot and restart after a crash: Kafka, the streaming job, the live producer, and the dashboard. Run this as the user who owns `~/vessel-watch`; the services run as that user (by numeric id, since a login name like `name@school.edu` may not be accepted by systemd).

```bash
unit() {  # name, make target, units to start after, extra [Service] lines
sudo tee /etc/systemd/system/vessel-$1.service >/dev/null <<EOF
[Unit]
Description=vessel-watch $1
After=docker.service network-online.target $3
[Service]
User=$(id -u)
WorkingDirectory=$HOME/vessel-watch
Environment=HOME=$HOME
Environment=PATH=$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin
Environment=PYTHONUNBUFFERED=1
ExecStart=/usr/bin/make $2
$4
[Install]
WantedBy=multi-user.target
EOF
}
unit kafka  kafka-up "" "Type=oneshot
RemainAfterExit=yes
SupplementaryGroups=docker"
unit stream stream "vessel-kafka.service" "Restart=always
RestartSec=15"
unit live   live   "vessel-kafka.service" "Restart=always
RestartSec=15"
unit app    app    "" "Restart=always
RestartSec=5"

sudo systemctl daemon-reload
sudo systemctl enable --now vessel-kafka vessel-stream vessel-live vessel-app
```

Check them:

```bash
systemctl status 'vessel-*' --no-pager
```

Stream, live, and app should be `active (running)`; kafka shows `active (exited)`, because it only starts the container and creates the topics. On the first start, stream and live retry every 15 seconds until Kafka is up.

After about two minutes, check that the stream is writing (it writes one file per minute; a live run prints no "Ready" line):

```bash
find ~/vessel-watch/data/serving/positions -name '*.parquet' -mmin -5 | wc -l
sudo journalctl -u vessel-live -n 5 --no-pager
```

The Kafka service gets Docker access through `SupplementaryGroups=docker`, because adding a school-login account to the `docker` group in step 2 does not always take effect. To run `docker` commands by hand in that case, use `sudo docker ...`.

The first start of the streaming job is slower because Spark downloads the Kafka connector.

## 6. HTTPS and the public address (on the server)

```bash
sudo tee /etc/caddy/Caddyfile >/dev/null <<'EOF'
HOST {
    reverse_proxy localhost:8501
}
EOF
sudo systemctl restart caddy
```

Replace `HOST` with the real DNS name before running this; a wrong name makes the certificate request fail (see `sudo journalctl -u caddy -n 20`). Caddy requests the Let's Encrypt certificate and renews it by itself, and redirects HTTP to HTTPS. The dashboard is then at `https://HOST`.

## 7. Confirm it fits in 4 GB

After about 15 minutes, and again after the first hourly compaction:

```bash
free -h
sudo docker stats --no-stream
ps -eo rss,comm --sort=-rss | head -5      # memory in KB per process
ps -eo args | grep -o -- '-Xmx[0-9a-zA-Z]*'  # heap limits in effect: expect 1536m (Spark) and 512m (Kafka)
```

B2s is enough if "available" in `free -h` stays above a few hundred MB and swap use stays near zero. If not, resize to B2ms (8 GB) in the portal under the VM's Size page; it only needs a restart.

For reference, the laptop measurement on Nov 11, before the memory caps, was about 3.5 GB in total: Spark 1.9 GB, Kafka 1.1 GB, the dashboard 0.2 GB. The caps are Kafka's 512 MB heap in `docker-compose.yml` and `stream.driver_memory` in `config.yaml`.

## Day to day

- **Update the code:** `git pull` in `~/vessel-watch`, then `sudo systemctl restart vessel-stream vessel-app`. Restart `vessel-live` too if the producer changed, and run `make env` first if dependencies changed.
- **New batch outputs** (lanes, shutdown spots, more NOAA days): rerun the `rsync` from step 4, then restart `vessel-stream` so it reads the new lanes.
- **Logs:** `sudo journalctl -u vessel-stream -n 100` (or `vessel-live`, `vessel-app`, `caddy`).
- **One live producer at a time.** Stop `make live` on the laptop while the server runs it; AISStream allows about three connections per account.
- **Credit.** B2s costs about $30 a month from the $100 student credit, plus a few dollars for the disk and the public IP. Stopping the VM in the portal (status "deallocated") pauses the compute charge.
