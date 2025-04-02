const express = require("express");
const zookeeper = require('node-zookeeper-client');
const { v4: uuidv4 } = require("uuid");

const ZK_SERVER = "127.0.0.1:2181";
const NODE_ID = `node-${uuidv4()}`;
const PORT = process.env.PORT || 8081;

let isLeader = false;
let liveNodes = [];
let persons = [];


let zkClient = zookeeper.createClient(ZK_SERVER);
// Initialize ZooKeeper Connection
zkClient.connect();

zkClient.once('connected', async () => {
    console.log(`Connected to ZooKeeper as ${NODE_ID}`);


// Ensure base znodes exist
await createZNode("/all_nodes", "persistent");
await createZNode("/live_nodes", "persistent");
await createZNode("/election", "persistent");

// Register this node in all_nodes (persistent)
await createZNode(`/all_nodes/${NODE_ID}`, "persistent", PORT);

// Register this node in live_nodes (ephemeral)
await createZNode(`/live_nodes/${NODE_ID}`, "ephemeral", PORT);

// Start leader election
await participateInLeaderElection();

// Watch live_nodes to track active servers
await watchLiveNodes();
});

async function createZNode(path, type, data = "") {
    try {
        await zkClient.create(path, data, { type });
    } catch (err) {
        if (err.code !== -110) console.error("ZNode creation failed:", err);
    }
}

async function participateInLeaderElection() {
    const leaderNode = await zkClient.create("/election/leader-", NODE_ID, { type: "ephemeralSequential" });
    const children = await zkClient.getChildren("/election");
    const sortedNodes = children.sort();
    isLeader = leaderNode.endsWith(sortedNodes[0]);
    console.log(isLeader ? "I am the leader" : "I am a follower");
}

async function watchLiveNodes() {
    liveNodes = await zkClient.getChildren("/live_nodes", async (event) => {
        console.log("Live nodes changed, updating...");
        await watchLiveNodes();
    });
}

const app = express();
app.use(express.json());

app.get("/cluster-info", (req, res) => {
    res.json({ nodeId: NODE_ID, isLeader, liveNodes });
});

app.get("/persons", (req, res) => {
    res.json(persons);
});

app.put("/persons", async (req, res) => {
    if (!isLeader) {
        return res.status(403).json({ error: "Only the leader can handle updates" });
    }
    persons.push(req.body);
    for (const node of liveNodes) {
        if (node !== NODE_ID) {
            await replicateData(node, req.body);
        }
    }
    res.status(201).json({ message: "Data updated" });
});

async function replicateData(node, data) {
    // Ideally, send a network request to replicate data to node
    console.log(`Replicating data to ${node}:`, data);
}

app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
