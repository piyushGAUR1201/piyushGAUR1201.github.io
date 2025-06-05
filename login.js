// ingestion_api.js
const express = require('express');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(express.json());

const PORT = 3000;

// In-memory store
const ingestions = {};
const priorityQueues = {
  HIGH: [],
  MEDIUM: [],
  LOW: []
};

const PRIORITY_ORDER = ['HIGH', 'MEDIUM', 'LOW'];

function createBatches(ids, size = 3) {
  const result = [];
  for (let i = 0; i < ids.length; i += size) {
    result.push(ids.slice(i, i + size));
  }
  return result;
}

function determineStatus(batchStatuses) {
  if (batchStatuses.every(s => s === 'yet_to_start')) return 'yet_to_start';
  if (batchStatuses.every(s => s === 'completed')) return 'completed';
  return 'triggered';
}

function processBatch() {
  for (const priority of PRIORITY_ORDER) {
    if (priorityQueues[priority].length > 0) {
      const { ingestionId, batchId, batch } = priorityQueues[priority].shift();
      ingestions[ingestionId].batches[batchId].status = 'triggered';

      setTimeout(() => {
        ingestions[ingestionId].batches[batchId].status = 'completed';
      }, 2000);

      break;
    }
  }
}

setInterval(processBatch, 5000); // Run every 5 seconds

// Ingestion: 
app.post('/ingest', (req, res) => {
  const { ids, priority } = req.body;
  const ingestionId = uuidv4();
  const batches = createBatches(ids);

  ingestions[ingestionId] = {
    ingestionId,
    priority,
    batches: {}
  };

  batches.forEach((batch, index) => {
    const batchId = uuidv4();
    ingestions[ingestionId].batches[batchId] = {
      ids: batch,
      status: 'yet_to_start'
    };
    priorityQueues[priority].push({ ingestionId, batchId, batch });
  });

  res.json({ ingestionId });
});

// Statuses
app.get('/status/:ingestionId', (req, res) => {
  const { ingestionId } = req.params;
  const record = ingestions[ingestionId];
  if (!record) {
    return res.status(404).json({ error: 'Ingestion ID not found' });
  }

  const batchStatuses = Object.values(record.batches).map(b => b.status);
  const overallStatus = determineStatus(batchStatuses);

  res.json({
    ingestionId,
    status: overallStatus,
    batches: Object.entries(record.batches).map(([batchId, info]) => ({
      batchId,
      ids: info.ids,
      status: info.status
    }))
  });
});

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});