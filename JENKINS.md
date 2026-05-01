# Jenkins Pipeline Configuration

This document describes the Jenkins pipeline configuration for the TradingPythonAgent project, including branch-aware deployments for development and production environments.

## Overview

The project uses a **branch-aware Jenkinsfile** that automatically detects the Git branch and configures the deployment environment accordingly. This allows for separate development and production deployments with minimal configuration.

## Files

- **`Jenkinsfile`** - Branch-aware pipeline (automatically handles `main`, `staging`, and feature branches)

## Branch-Aware Configuration

The main `Jenkinsfile` automatically detects the branch and applies the appropriate configuration:

### Feature Branches (`dev/TPA-{issue_number}/{project}-{subproject}`)

Feature branches must follow the pattern: `dev/TPA-{issue_number}/{project}-{subproject}`

**Pattern Format:**
- `dev/` - Prefix indicating development branch
- `TPA-{issue_number}` - OpenProject issue number (e.g., `TPA-38`, `TPA-45`, `TPA-100`)
- `{project}` - Project name (e.g., `trading_agent`)
- `{subproject}` - Subproject/module name (e.g., `fundamentals`, `macro`, `model`)

**Examples:**
- `dev/TPA-38/trading_agent-fundamentals`
- `dev/TPA-45/trading_agent-macro`
- `dev/TPA-100/trading_agent-model`

**Requirements:**
- The OpenProject issue **must exist** in your OpenProject instance before the build can proceed
- The pipeline will fail if the OpenProject issue does not exist (HTTP 404)
- The OpenProject issue must be accessible with the provided credentials

**Environment Variables Required:**
- `OPENPROJECT_URL` - OpenProject instance URL (e.g., `http://openproject.local.info`) - **without trailing slash**
- `OPENPROJECT_USER` - OpenProject username (e.g., `admin`)
- `OPENPROJECT_PASSWORD` - OpenProject password

**Configuration Methods:**

You can configure these variables in one of the following ways:

1. **Jenkins UI (Recommended for Development):**
   - Go to Jenkins → Manage Jenkins → Configure System
   - Under "Global properties", check "Environment variables"
   - Add: `OPENPROJECT_URL`, `OPENPROJECT_USER`, `OPENPROJECT_PASSWORD`
   - Or configure per-job in the job's configuration

2. **Jenkins Credentials Store (Recommended for Production):**
   - Store credentials in Jenkins Credentials store (ID: `openproject-password`)
   - Use `withCredentials` in Jenkinsfile to inject them as environment variables
   - More secure but requires Jenkinsfile modification

3. **Docker Compose (Quick Setup):**
   - Add environment variables to the Jenkins service in `.ops/.docker/docker-compose.yml`
   - Restart Jenkins: `docker-compose restart jenkins`

When the pipeline runs on a feature branch:

- **Image Name**: `hmm-model-training-dev`
- **Namespace**: `trading-monitoring-dev`
- **Job Name**: `hmm-model-calibration-dev`
- **Image Tags**:
  - Build tag: `dev-<BUILD_NUMBER>-<GIT_COMMIT_SHORT>`
  - Latest tag: `dev-latest`
- **Kubernetes Context**: `kind-trading-cluster`
- **Module Path**: `{project}/{subproject}` (e.g., `trading_agent/fundamentals`)

### Staging Branch (`staging`)

When the pipeline runs on the `staging` branch:

- **Image Name**: `hmm-model-training`
- **Namespace**: `trading-monitoring`
- **Job Name**: `hmm-model-calibration`
- **Image Tags**:
  - Build tag: `<BUILD_NUMBER>-<GIT_COMMIT_SHORT>`
  - Latest tag: `latest`
- **Kubernetes Context**: `kind-trading-cluster`

## Pipeline Stages

The pipeline consists of the following stages:

### 1. Checkout
- Checks out the source code from Git
- Detects the current branch
- Parses branch pattern: `dev/TPA-{issue_number}/{project}-{subproject}` for feature branches
- Sets environment variables based on branch detection
- Logs the detected environment and configuration

### 2. Test OpenProject Connection (Feature Branches Only)
- Tests connectivity to OpenProject instance
- Validates authentication credentials
- Makes an API call to OpenProject to verify connection
- **Build continues with warnings if:**
  - Authentication fails (HTTP 401/403)
  - OpenProject URL is invalid or unreachable
- Requires environment variables: `OPENPROJECT_URL`, `OPENPROJECT_USER`, `OPENPROJECT_PASSWORD`

### 3. Validate OpenProject Issue (Feature Branches Only)
- Validates that the OpenProject issue exists in your OpenProject instance
- Makes an API call to OpenProject to check issue existence
- **Build fails if:**
  - OpenProject issue does not exist (HTTP 404)
  - Authentication fails (HTTP 401/403)
  - OpenProject URL is invalid or unreachable
- Requires environment variables: `OPENPROJECT_URL`, `OPENPROJECT_USER`, `OPENPROJECT_PASSWORD`

### 4. Validate Module (Feature Branches Only)
- Validates that the module path exists in the codebase
- Checks that `src/{project}/{subproject}` directory exists
- Fails the build if the module path is invalid

### 5. Build Docker Image
- Builds the Docker image using `.ops/.kubernetes/Dockerfile.model-training`
- Tags the image with:
  - Build-specific tag (includes branch, build number, and commit SHA)
  - Latest tag (branch-specific: `latest` for main, `dev-latest` for dev)

### 6. Load Image into Kind Cluster
- Loads both the build-specific and latest tags into the local kind cluster
- Makes the images available for Kubernetes deployments

### 7. Create Namespace (if needed)
- Automatically creates the target namespace if it doesn't exist
- Uses the appropriate namespace based on branch:
  - `trading-monitoring-dev` for staging branch and feature branches (`dev/*`)
  - `trading-monitoring` for main branch

### 8. Update Kubernetes Job
- Updates or creates the Kubernetes Job with the new image
- Applies the job manifest from `.ops/.kubernetes/hmm-model-training-job.yaml`
- Uses the appropriate namespace and job name based on branch

### 9. Verify Deployment
- Verifies the job was created/updated successfully
- Lists pods to confirm deployment status

## Prerequisites

Before using the Jenkins pipeline, ensure:

1. **Jenkins is running** (deployed in Kubernetes or standalone)
   ```bash
   kubectl get pods -n jenkins
   ```

2. **Kind cluster is running** and accessible
   ```bash
   kind get clusters
   kubectl cluster-info --context kind-trading-cluster
   ```

3. **Docker is accessible** from Jenkins agent/node
   - Jenkins agent must be able to run `docker build` commands
   - Docker socket access or Docker-in-Docker setup

4. **Kubernetes configuration** is available
   - `kubectl` configured with access to `kind-trading-cluster`
   - Service account with appropriate permissions

5. **Dockerfile exists** at `.ops/.kubernetes/Dockerfile.model-training`

6. **Kubernetes job manifest exists** at `.ops/.kubernetes/hmm-model-training-job.yaml`

## Jenkins Job Configuration

### Setting up the Pipeline Job

1. **Create a new Pipeline job** in Jenkins

2. **Configure the job**:
   - **Pipeline Definition**: Pipeline script from SCM
   - **SCM**: Git
   - **Repository URL**: Your Git repository URL
   - **Credentials**: Add if repository is private
   - **Branches to build**: 
     - For main: `*/main`
     - For dev: `*/dev`
     - Or use multibranch pipeline for automatic branch detection

3. **Build Triggers** (optional):
   - Poll SCM: `H/5 * * * *` (poll every 5 minutes)
   - GitHub webhook (recommended)
   - Manual build

### Multibranch Pipeline (Recommended)

For automatic branch detection and separate builds per branch:

1. Create a **Multibranch Pipeline** job
2. Configure SCM (Git repository)
3. Jenkins will automatically:
   - Detect branches
   - Create separate jobs for each branch
   - Use the appropriate configuration based on branch name

## Usage Examples

### Manual Build from Jenkins UI

1. Navigate to your Jenkins job
2. Click "Build Now"
3. Jenkins will:
   - Detect the branch from the checkout
   - Apply the appropriate environment configuration
   - Build and deploy to the correct namespace

### Triggering via Git Push

1. Push changes to `staging` branch:
   ```bash
   git push origin staging
   ```

2. If webhook is configured, Jenkins automatically:
   - Detects the push to `staging` branch
   - Triggers the pipeline
   - Deploys to `trading-monitoring-dev` namespace

3. Push changes to a feature branch:
   ```bash
   git push origin dev/TPA-38/trading_agent-fundamentals
   ```

4. Jenkins automatically:
   - Validates the OpenProject issue exists
   - Triggers the pipeline
   - Deploys to `trading-monitoring-dev` namespace

5. Push changes to `main` branch:
   ```bash
   git push origin main
   ```

4. Jenkins automatically:
   - Detects the push to `main` branch
   - Triggers the pipeline
   - Deploys to `trading-monitoring` namespace

### Checking Deployment Status

After a build, verify the deployment:

```bash
# For dev environment
kubectl get job hmm-model-calibration-dev -n trading-monitoring-dev --context kind-trading-cluster
kubectl get pods -l app=hmm-model,component=training -n trading-monitoring-dev --context kind-trading-cluster

# For production/main environment
kubectl get job hmm-model-calibration -n trading-monitoring --context kind-trading-cluster
kubectl get pods -l app=hmm-model,component=training -n trading-monitoring --context kind-trading-cluster
```

## Image Management

### Image Tags

The pipeline creates two tags for each build:
- **Build-specific tag**: Includes branch, build number, and commit SHA
  - Dev: `hmm-model-training-dev:dev-123-abc1234`
  - Main: `hmm-model-training:456-def5678`
- **Latest tag**: Always points to the most recent build
  - Dev: `hmm-model-training-dev:dev-latest`
  - Main: `hmm-model-training:latest`

### Image Cleanup

The pipeline includes automatic cleanup of old images:
- Keeps the last 10 builds per environment
- Older images are automatically removed
- Runs in the `always` post-build step

### Viewing Images

```bash
# List dev images
docker images hmm-model-training-dev

# List production images
docker images hmm-model-training

# List all images
docker images | grep hmm-model-training
```

## Troubleshooting

### Pipeline Fails at Checkout

- **Issue**: Cannot access Git repository
- **Solution**: 
  - Check Jenkins credentials configuration
  - Verify repository URL is correct
  - Ensure Jenkins agent has network access

### Docker Build Fails

- **Issue**: `docker build` command fails
- **Solution**:
  - Verify Dockerfile exists at `.ops/.kubernetes/Dockerfile.model-training`
  - Check Docker daemon is running on Jenkins agent
  - Verify Docker socket permissions

### Image Cannot Be Loaded into Kind

- **Issue**: `kind load docker-image` fails
- **Solution**:
  - Verify kind cluster is running: `kind get clusters`
  - Check cluster name matches `trading-cluster`
  - Ensure Jenkins agent can access the kind cluster

### Kubernetes Job Creation Fails

- **Issue**: `kubectl apply` fails
- **Solution**:
  - Verify kubectl is configured: `kubectl cluster-info --context kind-trading-cluster`
  - Check job manifest exists: `.ops/.kubernetes/hmm-model-training-job.yaml`
  - Verify namespace exists or can be created
  - Check service account permissions

### Wrong Environment Detected

- **Issue**: Pipeline uses wrong namespace/image names
- **Solution**:
  - Check the branch name in Jenkins build logs
  - Verify `env.GIT_BRANCH` is set correctly
  - Review the branch detection logic in the Checkout stage

## Environment Variables

The pipeline sets the following environment variables:

| Variable | Dev Branch | Main Branch |
|----------|-----------|-------------|
| `ENV_SUFFIX` | `dev` | (empty) |
| `IMAGE_NAME` | `hmm-model-training-dev` | `hmm-model-training` |
| `NAMESPACE` | `trading-monitoring-dev` | `trading-monitoring` |
| `JOB_NAME` | `hmm-model-calibration-dev` | `hmm-model-calibration` |
| `IMAGE_TAG` | `dev-<BUILD>-<COMMIT>` | `<BUILD>-<COMMIT>` |
| `KIND_CLUSTER` | `trading-cluster` | `trading-cluster` |
| `GIT_BRANCH` | `dev` (or branch name) | `main` (or branch name) |
| `GIT_COMMIT_SHORT` | First 7 chars of commit SHA | First 7 chars of commit SHA |

## Best Practices

1. **Use Multibranch Pipelines**: Automatically handles multiple branches
2. **Set up Webhooks**: Automatically trigger builds on push
3. **Monitor Build Logs**: Check Jenkins console output for issues
4. **Verify Deployments**: Always check Kubernetes resources after deployment
5. **Keep Images Clean**: Rely on automatic cleanup, but monitor disk space
6. **Test in Dev First**: Always test changes in `dev` branch before merging to `main`
7. **Use Descriptive Commits**: Commit messages help track which builds correspond to which changes

## Related Documentation

- Kubernetes deployment: `.ops/.kubernetes/`
- Docker configuration: `.ops/.kubernetes/Dockerfile.model-training`
- Kubernetes job manifest: `.ops/.kubernetes/hmm-model-training-job.yaml`

## Support

For issues or questions:
1. Check Jenkins build logs for detailed error messages
2. Verify all prerequisites are met
3. Review Kubernetes and Docker logs
4. Check branch detection logic in pipeline code

