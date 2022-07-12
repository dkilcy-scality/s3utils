const getRepairStrategy = require('../../../../CompareRaftMembers/RepairObjects/getRepairStrategy');

describe('RepairObjects.getRepairStrategy()', () => {
    [
        {
            desc: 'missing on follower',
            followerState: {
                diffMd: null,
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Leader',
        },
        {
            desc: 'unreadable on follower',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Leader',
        },
        {
            desc: 'missing on follower and changed on leader',
            followerState: {
                diffMd: null,
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on follower and changed on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on follower and reappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'missing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: null,
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Follower',
        },
        {
            desc: 'unreadable on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'AutoRepair',
            expectedRepairSource: 'Follower',
        },
        {
            desc: 'missing on leader but reappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on leader but changed on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on leader but disappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: null,
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'readable on leader and follower',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'ManualRepair',
        },
        {
            desc: 'readable on leader and follower but changed on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'readable on leader and follower but disappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: true,
                refreshedMd: null,
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'missing on follower and unreadable on leader',
            followerState: {
                diffMd: null,
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'NotRepairable',
        },
        {
            desc: 'missing on leader and unreadable on follower',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: null,
                isReadable: false,
                refreshedMd: null,
            },
            expectedRepairStatus: 'NotRepairable',
        },
        {
            desc: 'unreadable on both leader and follower',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"foo":"bar"}',
            },
            expectedRepairStatus: 'NotRepairable',
        },
        {
            desc: 'unreadable on both leader and follower but changed on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: '{"newfoo":"newbar"}',
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'unreadable on both leader and follower but disappearing on leader',
            followerState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
            },
            leaderState: {
                diffMd: '{"foo":"bar"}',
                isReadable: false,
                refreshedMd: null,
            },
            expectedRepairStatus: 'UpdatedByClient',
        },
        {
            desc: 'readable on both leader and follower but follower too recent',
            followerState: {
                diffMd: '{"foo":"bar","last-modified":"2022-07-13T00:00:00.000Z"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar","last-modified":"2022-07-11T00:00:00.000Z"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar","last-modified":"2022-07-11T00:00:00.000Z"}',
            },
            olderThan: new Date('2022-07-12T00:00:00.000Z'),
            expectedRepairStatus: 'TooRecent',
        },
        {
            desc: 'readable on both leader and follower but leader too recent',
            followerState: {
                diffMd: '{"foo":"bar","last-modified":"2022-07-11T00:00:00.000Z"}',
                isReadable: true,
            },
            leaderState: {
                diffMd: '{"foo":"bar","last-modified":"2022-07-13T00:00:00.000Z"}',
                isReadable: true,
                refreshedMd: '{"foo":"bar","last-modified":"2022-07-13T00:00:00.000Z"}',
            },
            olderThan: new Date('2022-07-12T00:00:00.000Z'),
            expectedRepairStatus: 'TooRecent',
        },
    ].forEach(testCase => {
        const sourceDesc = testCase.expectedRepairSource
            ? ` from ${testCase.expectedRepairSource} source` : '';
        const testDesc = `object ${testCase.desc} should give status `
        + `${testCase.expectedRepairStatus}${sourceDesc}`;
        test(testDesc, () => {
            const repairStrategy = getRepairStrategy(
                testCase.followerState,
                testCase.leaderState,
                testCase.olderThan,
            );
            expect(repairStrategy.status).toEqual(testCase.expectedRepairStatus);
            expect(repairStrategy.source).toEqual(testCase.expectedRepairSource);
        });
    });
});
