const { versioning } = require('arsenal');

const VID_SEP = versioning.VersioningConstants.VersionId.Separator;

function getRepairStrategy(followerState, leaderState, olderThan) {
    if (leaderState.refreshedMd) {
        if (!leaderState.diffMd) {
            return {
                status: 'UpdatedByClient',
                message: 'changed on leader\'s view (exists but expected to be missing)',
            };
        }
        if (leaderState.diffMd !== leaderState.refreshedMd) {
            return {
                status: 'UpdatedByClient',
                message: 'changed on leader\'s view (metadata changed)',
            };
        }
    } else {
        if (leaderState.diffMd) {
            return {
                status: 'UpdatedByClient',
                message: 'changed on leader\'s view (missing but expected to exist)',
            };
        }
    }
    const isTooRecent = diffMd => {
        const parsedDiffMd = JSON.parse(diffMd);
        if (parsedDiffMd['last-modified']) {
            const lastModified = new Date(parsedDiffMd['last-modified']);
            return lastModified >= olderThan;
        }
        return false;
    };
    const leaderTooRecent = leaderState.isReadable && isTooRecent(leaderState.diffMd);
    const followerTooRecent = followerState.isReadable && isTooRecent(followerState.diffMd);
    if (leaderTooRecent || followerTooRecent) {
        return {
            status: 'TooRecent',
            message: 'last-modified too recent, not repairing',
        };
    }
    if (!followerState.diffMd && !leaderState.isReadable) {
        return {
            status: 'NotRepairable',
            message: 'absent from follower\'s view and not readable from leader\'s view',
        };
    }
    if (!leaderState.diffMd && !followerState.isReadable) {
        return {
            status: 'NotRepairable',
            message: 'absent from leader\'s view and not readable from follower\'s view',
        };
    }
    if (!leaderState.isReadable && !followerState.isReadable) {
        return {
            status: 'NotRepairable',
            message: 'not readable from neither leader\'s view nor follower\'s view',
        };
    }
    if (leaderState.isReadable && followerState.isReadable) {
        return {
            status: 'ManualRepair',
            message: 'readable from both leader\'s view and follower\'s view but metadata is different',
        };
    }
    return {
        status: 'AutoRepair',
        source: followerState.isReadable ? 'Follower' : 'Leader',
    };
}

module.exports = getRepairStrategy;
