import { analytics, createApp, server } from '@databricks/appkit';
import { erasureActions } from './plugins/erasure-actions';

await createApp({
  plugins: [server(), analytics(), erasureActions()],
}).catch((error: unknown) => {
  console.error('Failed to start EuroStream Databricks App', error);
  process.exitCode = 1;
});
