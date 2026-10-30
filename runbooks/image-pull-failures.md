# ErrImagePull / ImagePullBackOff

## Symptoms
Pods stuck in ErrImagePull or ImagePullBackOff. Events: "manifest unknown", "not found", "pull access denied".

## Diagnosis
If events show manifest unknown or tag not found right after a deploy, the new image tag does not exist in the
registry (typo, or CI never pushed it). If the event says unauthorized or pull access denied, the cause is registry
credentials (imagePullSecret), which is not fixable by changing the image.

## Fix
Bad tag after a recent rollout: roll back to the previous revision (action rollback), or set_image to the last
known good tag from the rollout history (keep the same repository). Credentials problems: escalate.
