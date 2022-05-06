import { expect as expectCDK, matchTemplate, MatchStyle } from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import * as SfSyncConnector from '../lib/sf-sync-connector-stack';

test('Empty Stack', () => {
    const app = new cdk.App();
    // WHEN
    const stack = new SfSyncConnector.SfSyncConnectorStack(app, 'MyTestStack');
    // THEN
    expectCDK(stack).to(matchTemplate({
      "Resources": {}
    }, MatchStyle.EXACT));
});
