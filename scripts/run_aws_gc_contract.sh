#!/usr/bin/env bash
set -euo pipefail

if [ -z "${EXPECTED_AWS_ACCOUNT_ID:-}" ]; then
    echo "ERROR: EXPECTED_AWS_ACCOUNT_ID environment variable is required (e.g. EXPECTED_AWS_ACCOUNT_ID=123456789012)" >&2
    exit 1
fi

EXPECTED_ACCOUNT="$EXPECTED_AWS_ACCOUNT_ID"
EXPECTED_REGION="${AWS_REGION:-us-east-1}"
SAFE_PREFIX="registry-rust-gc-contract-"

echo "=== 1. Verifying AWS STS Caller Identity ==="
CALLER_JSON="$(aws sts get-caller-identity --output json)"
CALLER_ARN="$(echo "$CALLER_JSON" | grep -o '"Arn": "[^"]*' | cut -d'"' -f4)"
CALLER_ACCOUNT="$(echo "$CALLER_JSON" | grep -o '"Account": "[^"]*' | cut -d'"' -f4)"

echo "Caller ARN: $CALLER_ARN"
echo "Caller Account: $CALLER_ACCOUNT"

if [ "$CALLER_ACCOUNT" != "$EXPECTED_ACCOUNT" ]; then
    echo "ERROR: AWS Account '$CALLER_ACCOUNT' does not match expected '$EXPECTED_ACCOUNT'" >&2
    exit 1
fi

AWS_REGION="${AWS_REGION:-$EXPECTED_REGION}"
if [ "$AWS_REGION" != "$EXPECTED_REGION" ]; then
    echo "ERROR: AWS Region '$AWS_REGION' does not match expected '$EXPECTED_REGION'" >&2
    exit 1
fi

# Generate unique bucket name
RUN_UUID="$(python3 -c "import uuid; print(uuid.uuid4())")"
BUCKET="${SAFE_PREFIX}${RUN_UUID}"

echo "=== 2. Creating Ephemeral Test Bucket: $BUCKET ==="
aws s3api create-bucket --bucket "$BUCKET" --region "$EXPECTED_REGION" --output json

cleanup() {
    local exit_code=$?
    echo "=== Cleaning up Bucket: $BUCKET (Exit Code: $exit_code) ==="
    if [[ "$BUCKET" != "$SAFE_PREFIX"* ]]; then
        echo "SAFETY REFUSAL: Bucket '$BUCKET' does not begin with prefix '$SAFE_PREFIX'" >&2
        return 1
    fi

    # Purge any objects/markers in the ephemeral bucket
    aws s3 rm "s3://$BUCKET" --recursive >/dev/null 2>&1 || true

    # Delete all versions and delete markers if versioning was touched
    VERSIONS_JSON="$(aws s3api list-object-versions --bucket "$BUCKET" --output json 2>/dev/null || true)"
    if [ -n "$VERSIONS_JSON" ]; then
        python3 -c '
import sys, json, subprocess
raw = sys.stdin.read()
if raw.strip():
    data = json.loads(raw)
    objects = []
    for v in data.get("Versions", []):
        objects.append({"Key": v["Key"], "VersionId": v["VersionId"]})
    for m in data.get("DeleteMarkers", []):
        objects.append({"Key": m["Key"], "VersionId": m["VersionId"]})
    if objects:
        payload = json.dumps({"Objects": objects, "Quiet": True})
        subprocess.run(["aws", "s3api", "delete-objects", "--bucket", "'"$BUCKET"'", "--delete", payload], check=False)
' <<< "$VERSIONS_JSON" 2>/dev/null || true
    fi

    # Delete the bucket
    aws s3api delete-bucket --bucket "$BUCKET" --region "$EXPECTED_REGION" >/dev/null 2>&1 || true

    # Verify 404 deletion with retry loop for eventual consistency
    local verified=0
    for _ in $(seq 1 15); do
        local head_out
        head_out="$(aws s3api head-bucket --bucket "$BUCKET" 2>&1 || true)"
        if echo "$head_out" | grep -q "Not Found\|404"; then
            echo "Bucket deletion verified (404 Not Found)."
            verified=1
            break
        fi
        sleep 1
    done

    if [ "$verified" -ne 1 ]; then
        echo "ERROR: Bucket $BUCKET head-bucket did not return 404 within verification timeout" >&2
        return 1
    fi

    return "$exit_code"
}
trap cleanup EXIT

echo "=== 3. Enabling S3 Public Access Block on $BUCKET ==="
aws s3api put-public-access-block \
    --bucket "$BUCKET" \
    --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

PAB="$(aws s3api get-public-access-block --bucket "$BUCKET" --output json)"
echo "Public access block active: $PAB"

echo "=== 4. Verifying Bucket Is Unversioned ==="
VERSIONING="$(aws s3api get-bucket-versioning --bucket "$BUCKET" --output json)"
echo "Bucket versioning query result: $VERSIONING"
if [ -n "$VERSIONING" ] && echo "$VERSIONING" | grep -q '"Status": "Enabled"'; then
    echo "ERROR: Ephemeral test bucket has versioning enabled unexpectedly" >&2
    exit 1
fi

echo "=== 5. Running Destructive S3 GC Contract Tests ==="
TEST_S3_BUCKET="$BUCKET" \
TEST_S3_REGION="$EXPECTED_REGION" \
TEST_S3_ENDPOINT="https://s3.amazonaws.com" \
TEST_S3_EXPECTED_ACCOUNT_ID="$EXPECTED_ACCOUNT" \
ALLOW_NON_LOCAL_S3_DESTRUCTIVE_TESTS=1 \
cargo test --locked --all-features --test s3_live_integration -- --nocapture

echo "=== Live AWS GC Contract Execution Succeeded ==="
