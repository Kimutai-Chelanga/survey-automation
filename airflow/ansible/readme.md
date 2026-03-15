Project Deployment on Contabo Server with Ansible
This guide provides a step-by-step process to deploy this project on a Contabo server using Ansible for automation. The project uses Docker and Docker Compose to manage services including Apache Airflow, Streamlit, MongoDB, PostgreSQL, and Automa with a noVNC GUI. It assumes you have a GitHub repository containing the necessary files (.devcontainer/Dockerfile, devcontainer.json, Dockerfile, Dockerfile.airflow, docker-compose.yml, requirements.txt, package.json, etc.) and are deploying on a Contabo VPS with Ubuntu 22.04 or 24.04.
Table of Contents

Prerequisites
Step 1: Purchase and Set Up the Contabo Server
Step 2: Install Ansible on the Control Node
Step 3: Prepare the GitHub Repository
Step 4: Create Ansible Playbooks
Step 5: Run Ansible Playbooks
Step 6: Verify the Deployment
Step 7: Automate Updates
Additional Considerations

Prerequisites

Contabo Server: A VPS or dedicated server with Ubuntu 22.04 or 24.04 (recommended: 4 CPU cores, 8 GB RAM, 200 GB SSD).
SSH Access: Access to the server’s IP address, root username, and password (provided by Contabo).
Control Node: A local machine or GitHub Codespace to run Ansible.
GitHub Repository: Contains all project files.
Knowledge: Basic understanding of SSH, Docker, Git, and Ansible.

Step 1: Purchase and Set Up the Contabo Server

Purchase the Server:

Visit Contabo and select a VPS or dedicated server.
Choose Ubuntu 22.04 or 24.04 during setup.
Receive the server’s IP address, root username, and password via email.


Access the Server:

Test SSH access:ssh root@<server-ip>

Replace <server-ip> with your server’s public IP.


Secure the Server:

Update the system:apt update && apt upgrade -y


Create a non-root user:adduser ansible-user
usermod -aG sudo ansible-user


Set up SSH key-based authentication:
Generate an SSH key on your control node (if needed):ssh-keygen -t rsa -b 4096


Copy the public key to the server:ssh-copy-id ansible-user@<server-ip>


Disable root login and password authentication:sudo nano /etc/ssh/sshd_config

Set:PermitRootLogin no
PasswordAuthentication no

Restart SSH:sudo systemctl restart ssh




Configure a firewall:sudo apt install ufw
sudo ufw allow OpenSSH
sudo ufw allow 80,443,8080,8501,27017,5432,6080,9222/tcp
sudo ufw enable





Step 2: Install Ansible on the Control Node

Choose a Control Node:

Use your local machine or GitHub Codespace.


Install Ansible:

Install Ansible:sudo apt update
sudo apt install software-properties-common
sudo add-apt-repository --yes --update ppa:ansible/ansible
sudo apt install ansible -y


Verify installation:ansible --version




Set Up Ansible Inventory:

Create an inventory file:mkdir ansible && cd ansible
nano inventory.yml


Add:all:
  hosts:
    contabo_server:
      ansible_host: <server-ip>
      ansible_user: ansible-user
      ansible_ssh_private_key_file: ~/.ssh/id_rsa

Replace <server-ip> with your server’s IP and verify the private key path.


Test Connectivity:

Run:ansible -i inventory.yml all -m ping


A pong response confirms connectivity. Troubleshoot SSH if it fails.



Step 3: Prepare the GitHub Repository

Verify Repository Contents:

Ensure your repository includes:
.devcontainer/Dockerfile
.devcontainer/devcontainer.json
Dockerfile
Dockerfile.airflow
docker-compose.yml
requirements.txt
package.json
src/, airflow/dags/, and other directories.


Commit and push changes:git add .
git commit -m "Prepare for Contabo deployment"
git push origin main




Create a .env File:

Create a .env file (add to .gitignore for security):nano .env

Example:AIRFLOW_FERNET_KEY=<generate-a-fernet-key>
GEMINI_API_KEY=<your-gemini-api-key>
AIRFLOW_UID=50000
AIRFLOW_GID=0


Generate a Fernet key:python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"





Step 4: Create Ansible Playbooks

Directory Structure:

Set up:ansible/
├── inventory.yml
├── playbooks/
│   ├── setup_server.yml
│   ├── deploy_app.yml
│   └── roles/
│       ├── common/
│       │   ├── tasks/
│       │   │   └── main.yml
│       ├── docker/
│       │   ├── tasks/
│       │   │   └── main.yml
│       └── app/
│           ├── tasks/
│           │   └── main.yml
│           ├── files/
│           │   └── .env
│           └── templates/
│               └── docker-compose.yml.j2




Common Role:

Create ansible/playbooks/roles/common/tasks/main.yml:- name: Update apt cache
  apt:
    update_cache: yes
    cache_valid_time: 3600
  become: yes

- name: Install required packages
  apt:
    name:
      - git
      - curl
      - wget
      - unzip
    state: present
  become: yes




Docker Role:

Create ansible/playbooks/roles/docker/tasks/main.yml:- name: Install Docker prerequisites
  apt:
    name:
      - ca-certificates
      - curl
      - gnupg
      - lsb-release
    state: present
  become: yes

- name: Add Docker GPG key
  shell: |
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
  become: yes

- name: Add Docker repository
  shell: |
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list
  become: yes

- name: Update apt cache after adding Docker repo
  apt:
    update_cache: yes
  become: yes

- name: Install Docker
  apt:
    name:
      - docker-ce
      - docker-ce-cli
      - containerd.io
      - docker-compose-plugin
    state: present
  become: yes

- name: Ensure Docker service is running
  systemd:
    name: docker
    state: started
    enabled: yes
  become: yes

- name: Add ansible-user to docker group
  user:
    name: ansible-user
    groups: docker
    append: yes
  become: yes




App Role:

Create ansible/playbooks/roles/app/tasks/main.yml:- name: Create application directory
  file:
    path: /home/ansible-user/app
    state: directory
    owner: ansible-user
    group: ansible-user
    mode: '0755'
  become: yes

- name: Clone GitHub repository
  git:
    repo: <your-github-repo-url>
    dest: /home/ansible-user/app
    version: main
    force: yes
  become: yes
  become_user: ansible-user

- name: Copy .env file
  copy:
    src: files/.env
    dest: /home/ansible-user/app/.env
    owner: ansible-user
    group: ansible-user
    mode: '0600'
  become: yes

- name: Template docker-compose.yml
  template:
    src: templates/docker-compose.yml.j2
    dest: /home/ansible-user/app/docker-compose.yml
    owner: ansible-user
    group: ansible-user
    mode: '0644'
  become: yes

- name: Create Docker network
  docker_network:
    name: app_network
    state: present
  become: yes

- name: Start Docker Compose services
  command: docker compose -f /home/ansible-user/app/docker-compose.yml up -d
  become: yes
  become_user: ansible-user


Replace <your-github-repo-url> with your repository URL (e.g., https://github.com/username/repo.git).
Copy your .env file to ansible/playbooks/roles/app/files/.env.
Copy docker-compose.yml to ansible/playbooks/roles/app/templates/docker-compose.yml.j2. Use Jinja2 variables for sensitive values if needed (e.g., {{ GEMINI_API_KEY }}).


Setup Server Playbook:

Create ansible/playbooks/setup_server.yml:- name: Set up Contabo server
  hosts: contabo_server
  roles:
    - common
    - docker




Deploy App Playbook:

Create ansible/playbooks/deploy_app.yml:- name: Deploy application
  hosts: contabo_server
  roles:
    - app





Step 5: Run Ansible Playbooks

Set Up the Server:

Run:ansible-playbook -i inventory.yml playbooks/setup_server.yml


This installs Git, Docker, and prerequisites.


Deploy the Application:

Run:ansible-playbook -i inventory.yml playbooks/deploy_app.yml


This clones the repository, copies .env, templates docker-compose.yml, creates the app_network, and starts services.



Step 6: Verify the Deployment

Check Docker Services:

SSH into the server:ssh ansible-user@<server-ip>


Verify services:cd ~/app
docker compose ps


Expected services: mongodb, postgres, airflow-init, airflow-webserver, airflow-scheduler, streamlit_app, db-backup (if enabled).


Access Services:

Airflow Web UI: http://<server-ip>:8080 (login: username brian, password kimu).
Streamlit: http://<server-ip>:8501.
noVNC GUI: http://<server-ip>:6080.
Chrome Debugging: Port 9222.
If ports are inaccessible, ensure applications bind to 0.0.0.0 and check Contabo’s firewall.


Troubleshoot:

Check logs:docker compose logs airflow-webserver
docker compose logs streamlit_app


Verify puppeteer-core:docker exec airflow_scheduler bash -c "cd /opt/airflow/src && npm list puppeteer-core"


Check /opt/airflow/logs (Airflow) or /app/logs (Streamlit) for errors.



Step 7: Automate Updates

Update Playbook:

Modify ansible/playbooks/deploy_app.yml for updates:- name: Deploy application
  hosts: contabo_server
  tasks:
    - name: Pull latest repository changes
      git:
        repo: <your-github-repo-url>
        dest: /home/ansible-user/app
        version: main
        force: yes
      become: yes
      become_user: ansible-user
    - name: Restart Docker Compose services
      command: docker compose -f /home/ansible-user/app/docker-compose.yml up -d --build
      become: yes
      become_user: ansible-user




Run Updates:

After pushing changes to GitHub:ansible-playbook -i inventory.yml playbooks/deploy_app.yml





Additional Considerations

Backups: The db-backup service creates backups in /backups. Copy them to Contabo Object Storage periodically.
Monitoring: Use Prometheus or Grafana to monitor server and container performance.
Security: Set up Nginx with Let’s Encrypt for HTTPS.
Scaling: Upgrade the VPS or add a load balancer for high traffic.
Chrome Profile: Ensure /home/ansible-user/app/chrome_profile exists with chmod 777.
Puppeteer Issues: Verify "puppeteer-core": "^21.0.0" in package.json and rebuild the Airflow image if needed.


