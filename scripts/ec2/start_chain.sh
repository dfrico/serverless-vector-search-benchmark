# Start the full baseline chain on the EC2 client inside tmux session "run".
# Sent via: harness/.venv/bin/python scripts/ec2/ssm_run.py scripts/ec2/start_chain.sh
# Stages run sequentially and stop at the first failure; results are pushed to S3
# after the chain ends either way (scripts/ec2/sync_results.sh pull 2026-10-ec2).
# The body is one function so bash parses it whole before the bundle overwrites this file
# (on 2026-10-17 a mid-run overwrite made bash run the old stage list).
main() {
set -e
cd ~/thesis
# Refresh the code from a new bundle if BUNDLE_KEY is set (code only; venvs and data stay).
if [ -n "${BUNDLE_KEY:-}" ]; then
  aws s3 cp --only-show-errors "s3://thesis-blocks-$(aws sts get-caller-identity --query Account --output text)/$BUNDLE_KEY" /tmp/b.tgz
  tar --warning=no-unknown-keyword -xzf /tmp/b.tgz && rm /tmp/b.tgz
  echo "code updated from $BUNDLE_KEY"
fi
TAG="${TAG:-2026-10-ec2}"   # replicates (D14) pass TAG=2026-10-repN
STAGES="${STAGES:-b c1 c2 c3 s3v_rate}"
# STOP_WHEN_DONE=1: stop (not terminate) the instance 5 min after the results push.
AFTER=""; [ "${STOP_WHEN_DONE:-0}" = 1 ] && AFTER="; sudo shutdown -h +5"
tmux kill-session -t run 2>/dev/null || true
tmux new-session -d -s run "cd ~/thesis && RESULTS_TAG=$TAG bash experiments/rerun_baseline.sh $STAGES 2>&1 | tee -a ~/chain.log; echo CHAIN_EXIT=\${PIPESTATUS[0]} | tee -a ~/chain.log; bash scripts/ec2/sync_results.sh push $TAG >> ~/chain.log 2>&1$AFTER"
sleep 2; echo "stages: $STAGES"; tmux ls; tail -3 ~/chain.log 2>/dev/null || true
}
main "$@"
