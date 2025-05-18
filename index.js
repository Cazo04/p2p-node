const si = require('systeminformation');
const io = require('socket.io-client');
const { createHash } = require('blake2');
const fs = require('fs');
const path = require('path');
const wrtc = require('wrtc');
const { EventEmitter, on } = require('events');
const net = require('net');

// Load configuration from file
const configFilePath = path.join(__dirname, 'node-settings.json');
let config;

// Check if config file exists
if (!fs.existsSync(configFilePath)) {
    console.log('Settings file not found! Creating template...');

    // Create default config template
    const defaultConfig = {
        "signaling_servers": [
            "https://p2p.cazo-dev.net/socket.io",
            "http://localhost:3000",
        ],
        "webrtc": {
            "iceServers": [
                {
                    "urls": "stun:stun.l.google.com:19302"
                }
            ]
        },
        "info": {
            "id": "",
            "auth_token": ""
        },
        "paths": [
            {
                "path": "/home/user/p2p-media-system/build-node-client/",
                "threshold": 80
            },
        ]
    };

    // Write the default config to file
    fs.writeFileSync(configFilePath, JSON.stringify(defaultConfig, null, 2));

    console.log('Settings template created at:', configFilePath);
    console.log('Please edit the settings file with your configuration and restart the application.');
    process.exit(1);
}

// Load the configuration
try {
    config = JSON.parse(fs.readFileSync(configFilePath));
} catch (error) {
    console.error('Error parsing settings file:', error.message);
    process.exit(1);
}

// Add global fragments map
const fragmentsMap = new Map();

const SIGNALING_URL = config.signaling_servers[0];
const socket = io(SIGNALING_URL, {
    path: '/socket.io',
});

// Handle connection errors and try alternative servers
let currentServerIndex = 0;
const maxServerIndex = config.signaling_servers.length - 1;

socket.on('connect_error', (err) => {
    console.error('Failed to connect to signaling server:', err.message);

    // Try the next server in the list
    if (currentServerIndex < maxServerIndex) {
        currentServerIndex++;
        const nextServer = config.signaling_servers[currentServerIndex];
        console.log(`Attempting to connect to alternative server: ${nextServer}`);

        // Close current connection
        socket.disconnect();

        // Update the URI and attempt reconnection
        socket.io.uri = nextServer;
        setTimeout(() => {
            console.log(`Reconnecting to ${nextServer}...`);
            socket.connect();
        }, 5000);
    } else {
        console.error("Tried all available signaling servers without success.");
        console.log("Retrying from the first server...");

        // Reset to the first server and try again
        currentServerIndex = 0;
        const firstServer = config.signaling_servers[currentServerIndex];
        socket.disconnect();
        socket.io.uri = firstServer;
        setTimeout(() => {
            console.log(`Reconnecting to ${firstServer}...`);
            socket.connect();
        }, 5000);
    }
});

// WebRTC configuration
const webrtcConfig = config.webrtc;

// Node information
const node_info = {
    id: config.info.id,
    auth_token: config.info.auth_token,
    connection_id: null
};

// Path information that will be monitored
const monitoredPaths = config.paths;

async function checkDiskSpace(storageInfo) {
    try {
        // Get information about the file system
        const fsInfo = await si.fsSize();

        // Find the file system containing the provided path
        const driveLetter = storageInfo.path.charAt(0).toUpperCase();
        console.log(`Drive letter: ${driveLetter}`);
        const relevantFs = fsInfo.find(fs => fs.mount && fs.mount.charAt(0).toUpperCase() === driveLetter);

        if (!relevantFs) {
            throw new Error(`Could not find filesystem for path: ${storageInfo.path}`);
        }

        const thresholdLimit = Math.floor((relevantFs.size * storageInfo.threshold) / 100);

        // Return values in bytes without conversion
        return {
            path: storageInfo.path,
            filesystem: relevantFs.fs,
            mount: relevantFs.mount,
            totalSizeBytes: thresholdLimit,
            availableSpaceBytes: Math.min(relevantFs.available, thresholdLimit),
            usedPercent: `${relevantFs.use}%`
        };
    } catch (error) {
        console.error('Error getting disk space information:', error.message);

    }
}

async function sendDiskSpace() {
    try {
        let diskSpaceInfo = [];
        for (const storageInfo of monitoredPaths) {
            const result = await checkDiskSpace(storageInfo);
            if (result) {
                diskSpaceInfo.push(result);
            }
        }

        // Filter out duplicate partitions (paths pointing to the same filesystem)
        const uniquePartitions = new Map();
        diskSpaceInfo.forEach(info => {
            // Use the mount point as the unique identifier for the partition        
            //console.log('info:', info);   
            if (!uniquePartitions.has(info.mount)) {
                uniquePartitions.set(info.mount, info);
            } else {
                if (uniquePartitions.get(info.mount).availableSpaceBytes > info.availableSpaceBytes) {
                    uniquePartitions.set(info.mount, info);
                }
            }
        });

        // Replace with filtered disk space info (unique partitions only)
        diskSpaceInfo = Array.from(uniquePartitions.values());
        //console.log(`Found ${diskSpaceInfo.length} unique partitions from ${monitoredPaths.length} monitored paths`);
        let deviceDisk = {
            totalSize: 0,
            availableSpace: 0
        };
        diskSpaceInfo.forEach(info => {
            deviceDisk.totalSize += info.totalSizeBytes;
            deviceDisk.availableSpace += info.availableSpaceBytes;
        });

        // Get memory information
        const memInfo = await si.mem();
        const ramUsagePercent = ((memInfo.total - memInfo.available) / memInfo.total) * 100;

        console.log(`Current RAM usage: ${ramUsagePercent.toFixed(2)}%`);

        const device_space = {
            node_info: node_info,
            disk_space: deviceDisk,
            ram_usage: ramUsagePercent.toFixed(2),
        };
        //console.log('Sending disk space information:', device_space);
        socket.emit('device_space', device_space);
        return true;
    } catch (error) {
        console.error('Error sending disk space information:', error.message);
        return false;
    }
}

function calculateBlake2bHash(filePath) {
    const fileData = fs.readFileSync(filePath);
    const h = createHash('blake2b', { digestLength: 32 });
    h.update(fileData);
    return h.digest('hex');
}

function sendListHashes() {
    isHashProcessing = true;

    const hashes = [];

    for (const info of monitoredPaths) {

        const files = fs.readdirSync(info.path);
        for (const file of files) {
            const filePath = info.path + '/' + file;
            const stats = fs.statSync(filePath);
            if (stats.isFile()) {
                const hash = calculateBlake2bHash(filePath);
                const fileHash = {
                    id: file,
                    hash: hash
                };
                hashes.push(fileHash);
            }
        }
    }

    const data = {
        node_info: node_info,
        hashes: hashes
    };
    //console.log('Sending list of hashes:', data);
    socket.emit('list_hashes', data);

    setTimeout(() => {
        isHashProcessing = false;
    }, 200);
}



socket.on('auth_response', (data) => {
    if (data.status === 'ok') {
        console.log('Authenticated successfully');
        startUp();
    } else
        if (data.status === 'id_or_token_error') {
            console.log('ID or token error. You need to leave ID or token empty to register.');
            process.exit(1);
        } else if (data.status === 'accept') {
            console.log('Registration successful. Saving new credentials...');

            // Update local node_info
            if (data.id) node_info.id = data.id;
            if (data.auth_token) node_info.auth_token = data.auth_token;

            // Update the config object
            config.info.id = node_info.id;
            config.info.auth_token = node_info.auth_token;

            // Save updated config back to file
            try {
                fs.writeFileSync(
                    configFilePath,
                    JSON.stringify(config, null, 2)
                );
                console.log('Credentials saved successfully!');
                startUp();
            } catch (error) {
                console.error('Error saving credentials:', error.message);
            }
        } else {
            console.log('Authentication failed with status:', data.status);
            console.log('Retrying in 5 seconds...');
            setTimeout(() => {
                console.log('Attempting to reconnect...');
                socket.emit('authenticate', node_info);
            }, 5000);
        }
});

// Variables to hold interval references
let diskSpaceInterval;
let hashesInterval;

let isCommandProcessing = false;
let isHashProcessing = false;

// Function to start periodic reporting
function startPeriodicReporting() {
    console.log('Starting periodic reporting...');

    // Send disk space info every 20 seconds as heartbeat
    diskSpaceInterval = setInterval(() => {
        if (!isCommandProcessing) {
            sendDiskSpace();
        } else {
            console.log('Command processing in progress, skipping disk space report');
        }
    }, 20000);

    // Send list of hashes every 5 minutes
    // hashesInterval = setInterval(() => {
    //     if (!isHashProcessing) {
    //         sendListHashes();
    //     } else {
    //         console.log('Hash processing in progress, skipping hashes report');
    //     }
    // }, 300000);
    sendListHashes();
}

// Clean up intervals on process exit
process.on('SIGINT', () => {
    if (diskSpaceInterval) clearInterval(diskSpaceInterval);
    if (hashesInterval) clearInterval(hashesInterval);
    console.log('Intervals cleared, exiting...');
    process.exit();
});

let isStartUp = false;
// Modify the startUp function to start intervals after completion
async function startUp() {
    if (isStartUp) return;
    isStartUp = true;

    console.log('Starting up node client...');
    console.log(`Node ID: ${node_info.id}`);
    console.log(`Monitored paths: ${monitoredPaths.map(item => item.path).join(', ')}`);

    // Check and create p2p-node-{id} folder for each path
    for (let i = 0; i < monitoredPaths.length; i++) {
        const pathObj = monitoredPaths[i];
        const basePath = pathObj.path;
        const nodeFolderName = `p2p-node-${node_info.id}`;
        const nodeFolderPath = path.join(basePath, nodeFolderName);

        console.log(`Checking if ${nodeFolderPath} exists...`);

        // Check if path format matches current OS (Windows uses \ and may have drive letters)
        const isWindowsPath = /^[a-zA-Z]:/.test(basePath) || basePath.includes('\\');
        const isCurrentOSWindows = path.sep === '\\';

        if ((isWindowsPath && !isCurrentOSWindows) || (!isWindowsPath && isCurrentOSWindows)) {
            console.log(`Removing ${basePath} from monitored paths due to incompatible OS format`);
            monitoredPaths.splice(i, 1);
            i--; // Adjust index since we're removing an element
            continue; // Skip this path
        }

        // Check if the directory exists
        if (!fs.existsSync(nodeFolderPath)) {
            console.log(`Creating directory: ${nodeFolderPath}`);
            try {
                fs.mkdirSync(nodeFolderPath, { recursive: true });
                console.log(`Created directory: ${nodeFolderPath}`);
            } catch (error) {
                console.error(`Error creating directory ${nodeFolderPath}:`, error.message);
                console.log(`Removing ${basePath} from monitored paths due to error`);
                monitoredPaths.splice(i, 1);
                i--; // Adjust index since we're removing an element
                continue; // Skip this path if there's an error
            }
        } else {
            console.log(`Directory already exists: ${nodeFolderPath}`);
        }

        // Update the path in monitoredPaths
        monitoredPaths[i].path = nodeFolderPath;
    }

    // Instead of deleting fragments.json
    // Clear the fragments map
    fragmentsMap.clear();

    // Rebuild fragments map by scanning all monitored paths
    console.log(`Building fragments map from monitored paths`);

    for (const pathObj of monitoredPaths) {
        const dirPath = pathObj.path;
        if (fs.existsSync(dirPath)) {
            const files = fs.readdirSync(dirPath);
            for (const file of files) {
                const filePath = path.join(dirPath, file);
                const stats = fs.statSync(filePath);
                if (stats.isFile() && !file.includes("fragments.json")) {
                    // Assume the filename is the fragment ID
                    const fragment_id = file;
                    fragmentsMap.set(fragment_id, { path: filePath });
                    console.log(`Added fragment ${fragment_id} to fragments map`);
                }
            }
        }
    }
    console.log(`Updated fragments map with ${fragmentsMap.size} fragments`);

    console.log(`Updated monitored paths: ${monitoredPaths.map(item => item.path).join(', ')}`);

    // Send disk space information to signaling server
    if (await sendDiskSpace()) {
        // Send list of hashes to signaling server
        sendListHashes();

        // Start periodic reporting after initial data is sent
        startPeriodicReporting();
    }
}

// Socket connection and authentication
socket.on('connect', () => {
    console.log('Connected to signaling server');

    // Send authentication data to server
    socket.emit('authenticate', node_info);

});

// const comming_commands = {
//     'remove_fragment': [
//         'fr_1',
//         'fr_2',
//     ],
//     'download_fragment': [
//         {
//             id: 'fr_1',
//             url: 'http://localhost:3000/files/fr_1'
//         }
//     ]
// };

// const finished_commands = {
//     'remove_status': [
//         {
//             id: 'fr_1',
//             status: 'ok'
//         },
//     ],
//     'download_status': [
//         {
//             id: 'fr_1',
//             status: 'ok'
//         }
//     ]
// }

// Function to get file size using HEAD request
async function getFileSize(url) {
    return new Promise((resolve, reject) => {
        const protocol = url.startsWith('https') ? require('https') : require('http');

        const req = protocol.request(url, {
            method: 'HEAD',
            headers: {
                'x-node-id': node_info.id
            }
        }, (res) => {
            if (res.statusCode !== 200) {
                reject(new Error(`HTTP error: ${res.statusCode}`));
                return;
            }

            const contentLength = res.headers['content-length'];
            if (contentLength) {
                resolve(parseInt(contentLength, 10));
            } else {
                reject(new Error('Content-Length header not found'));
            }
        });

        req.on('error', reject);
        req.end();
    });
}

// Function to find suitable path with enough space
async function findSuitablePath(fileSize) {
    for (const info of monitoredPaths) {
        try {
            const spaceInfo = await checkDiskSpace(info);
            if (spaceInfo && spaceInfo.availableSpaceBytes > fileSize) {
                //console.log(`Found suitable path ${info.path} with ${spaceInfo.availableSpaceBytes} bytes available`);
                return info.path;
            }
        } catch (error) {
            console.error(`Error checking space for path ${info.path}:`, error.message);
        }
    }
    return null;
}

// Function to download file
function downloadFile(fragment_id, url, destinationPath) {
    return new Promise((resolve, reject) => {
        const protocol = url.startsWith('https') ? require('https') : require('http');
        const filePath = path.join(destinationPath, fragment_id);
        const fileStream = fs.createWriteStream(filePath);

        const req = protocol.get(url, {
            headers: {
                'x-node-id': node_info.id
            }
        }, (res) => {
            if (res.statusCode !== 200) {
                fileStream.close();
                fs.unlink(filePath, () => { });
                reject(new Error(`HTTP error: ${res.statusCode}`));
                return;
            }

            res.pipe(fileStream);

            fileStream.on('finish', () => {
                fileStream.close();
                resolve(filePath);
            });
        });

        req.on('error', (err) => {
            fileStream.close();
            fs.unlink(filePath, () => { });
            reject(err);
        });

        fileStream.on('error', (err) => {
            fileStream.close();
            fs.unlink(filePath, () => { });
            reject(err);
        });
    });
}

socket.on('command', (data) => {
    isCommandProcessing = true;
    //console.log('Received command:', data);
    const delete_fragment = data.delete_fragment;
    const download_fragment = data.download_fragment;

    const status = {
        node_info: node_info,
        remove_status: [],
        download_status: []
    };

    if (delete_fragment) {
        // Create array to collect all removal statuses
        const removalPromises = delete_fragment.map(fragment_id => {
            return new Promise(resolve => {
                try {
                    // Check if fragment exists in our map
                    if (fragmentsMap.has(fragment_id)) {
                        const fragmentPath = fragmentsMap.get(fragment_id).path;

                        try {
                            // Delete the file
                            fs.unlinkSync(fragmentPath);

                            // Remove fragment from map
                            fragmentsMap.delete(fragment_id);

                            resolve({
                                id: fragment_id,
                                status: 'ok'
                            });
                        } catch (deleteErr) {
                            resolve({
                                id: fragment_id,
                                status: 'error',
                                error: deleteErr.message
                            });
                        }
                    } else {
                        resolve({
                            id: fragment_id,
                            status: 'error',
                            error: 'Fragment not found'
                        });
                    }
                } catch (err) {
                    console.error(`Error processing removal of fragment ${fragment_id}:`, err.message);
                    resolve({
                        id: fragment_id,
                        status: 'error',
                        error: err.message
                    });
                }
            });
        });

        // Wait for all removals to complete before sending status
        Promise.all(removalPromises)
            .then(results => {
                status.remove_status = results;
                //console.log('All fragment removals completed');
            })
            .catch(error => {
                console.error('Error processing removals:', error);
            });
    }

    if (download_fragment) {

        // Create array to collect all download promises
        const downloadPromises = download_fragment.map(fragment => {
            const fragment_id = fragment.id;
            const fragment_url = fragment.url;

            // Return a promise for this download operation
            return (async () => {
                //console.log(`Processing download for fragment ${fragment_id} from ${fragment_url}`);
                try {
                    const fileSize = await getFileSize(fragment_url);
                    //console.log(`Fragment ${fragment_id} has size: ${fileSize} bytes`);

                    const suitablePath = await findSuitablePath(fileSize);

                    if (!suitablePath) {
                        console.error(`No path with enough space for fragment ${fragment_id}`);
                        return {
                            id: fragment_id,
                            status: 'error',
                            error: 'No suitable path found'
                        };
                    }

                    //console.log(`Downloading fragment ${fragment_id} to ${suitablePath}`);
                    const filePath = await downloadFile(fragment_id, fragment_url, suitablePath);
                    console.log(`Successfully downloaded fragment ${fragment_id} to ${filePath}`);

                    // Add new fragment information
                    fragmentsMap.set(fragment_id, { path: filePath });

                    const hash = calculateBlake2bHash(filePath);
                    return {
                        id: fragment_id,
                        status: 'ok',
                        hash: hash
                    };
                } catch (error) {
                    console.error(`Error processing download for fragment ${fragment_id}:`, error.message);
                    return {
                        id: fragment_id,
                        status: 'error',
                        error: error.message
                    };
                }
            })();
        });

        // Wait for all downloads to complete before sending status
        Promise.all(downloadPromises)
            .then(results => {

                socket.emit('dowload_status', {
                    node_info: node_info,
                    download_status: results
                });
                console.log('All fragment downloads completed');
            })
            .catch(error => {
                console.error('Error processing downloads:', error);
            });
    }
    isCommandProcessing = false;

    // Send status after all commands have been processed

    // setTimeout(() => {
    //     isCommandProcessing = false;
    //     if (!isHashProcessing) {
    //         sendListHashes();
    //     }
    // }, 2000);
});

// WebRTC functionality for peer-to-peer file transfers
const { RTCPeerConnection, RTCSessionDescription, RTCIceCandidate } = wrtc;

let peerConnections = {};
let dataChannels = {};
let activeTransfers = {};
const peerActivity = new Map();
const peerStats = new Map();

// Function to create a new WebRTC peer connection
function createPeerConnection(peerId) {
    if (peerConnections[peerId]) {
        console.log(`Peer connection to ${peerId} already exists`);
        return peerConnections[peerId];
    }

    console.log(`Creating new peer connection to ${peerId}`);
    const peerConnection = new RTCPeerConnection(webrtcConfig);

    peerConnection.onicecandidate = (event) => {
        if (event.candidate) {
            socket.emit('ice_candidate', {
                target: peerId,
                candidate: event.candidate,
            });
        }
    };

    peerConnection.oniceconnectionstatechange = () => {
        console.log(`ICE connection state: ${peerConnection.iceConnectionState}`);
        if (['failed', 'disconnected', 'closed'].includes(peerConnection.iceConnectionState)) {
            cleanupPeerConnection(peerId);
        }
    };

    // const dataChannel = peerConnection.createDataChannel(`fileTransfer-${peerId}`);
    // setupDataChannel(dataChannel, peerId);

    peerConnections[peerId] = peerConnection;
    return peerConnection;
}

function classifyIp(ip) {
    const ver = net.isIP(ip);       
    if (ver === 0) return { version: 'unknown', type: 'unknown' };
  
    if (ver === 4) {
      return { version: 'IPv4', type: isPrivateV4(ip) ? 'private' : 'public' };
    }
  
    return { version: 'IPv6', type: isPrivateV6(ip) ? 'private' : 'public' };
  }
  
  function isPrivateV4(ip) {
    const [a, b] = ip.split('.').map(Number);
  
    // RFC 1918 private blocks
    if (a === 10) return true;                     // 10.0.0.0/8
    if (a === 172 && b >= 16 && b <= 31) return true; // 172.16.0.0/12
    if (a === 192 && b === 168) return true;       // 192.168.0.0/16
  
    if (a === 127) return true;                    // loopback 127.0.0.0/8
    if (a === 169 && b === 254) return true;       // link‑local 169.254.0.0/16
    if (a === 100 && b >= 64 && b <= 127) return true; // CGNAT 100.64.0.0/10
  
    return false;
  }
  
  function isPrivateV6(ip) {
  
    const addr = ip.split('%')[0].toLowerCase();
  
    if (addr.startsWith('fc') || addr.startsWith('fd')) return true;
  
    if (/^fe[89ab]/.test(addr)) return true;
  
    if (addr === '::1') return true;
  
    return false;
  }

async function pollPeer(p,id) {
    const report = await p.getStats();
    let rtt = null;
    let bytesSent = 0;
    let bytesReceived = 0;
    let remote_ipv4 = null;
    let remote_ipv6 = null;
    let local_ipv4 = null;
    let local_ipv6 = null;

    const peerStat = peerStats.get(id);

    report.forEach(s => {
        if (s.type === 'candidate-pair' && s.state === 'succeeded')
            rtt = s.currentRoundTripTime * 1000;

        if (s.type === 'data-channel' && s.state === 'open') {
            bytesSent = s.bytesSent;
            bytesReceived = s.bytesReceived;
        }
        if (s.type === 'remote-candidate') {
            const ip = s.ip;
            const { version, type } = classifyIp(ip);
            if (type === 'public') {
                if (version === 'IPv4') remote_ipv4 = ip;
                else remote_ipv6 = ip;
            }
        }
        if (s.type === 'local-candidate') {
            const ip = s.ip;
            const { version, type } = classifyIp(ip);
            if (type === 'public') {
                if (version === 'IPv4') local_ipv4 = ip;
                else local_ipv6 = ip;
            }
        }

        //console.log(`Peer stats:`, s);
    });

    // console.log(`RTT: ${rtt} ms`);
    // console.log(`Bytes sent: ${bytesSent - peerStat.bytesSent}`);
    // console.log(`Bytes received: ${bytesReceived - peerStat.bytesReceived}`);
    // console.log(`Remote IPv4: ${remote_ipv4}`);
    // console.log(`Remote IPv6: ${remote_ipv6}`);
    // console.log(`Local IPv4: ${local_ipv4}`);
    // console.log(`Local IPv6: ${local_ipv6}`);

    socket.emit('peer_stats', {
        peerId: id,
        rtt: rtt,
        bytesSent: bytesSent - peerStat.bytesSent,
        bytesReceived: bytesReceived - peerStat.bytesReceived,
        remote_ipv4: remote_ipv4,
        remote_ipv6: remote_ipv6,
        local_ipv4: local_ipv4,
        local_ipv6: local_ipv6,
    });

    peerStats.set(id, {
        ...peerStats.get(id),
        rtt: rtt,
        bytesSent: bytesSent,
        bytesReceived: bytesReceived,
        remote_ipv4: remote_ipv4,
        remote_ipv6: remote_ipv6,
        local_ipv4: local_ipv4,
        local_ipv6: local_ipv6
    });
}

function onNewPeer(peerId) {
    const peer = peerConnections[peerId];
  
    const id = setInterval(()=> pollPeer(peer,peerId), 1000);
  
    peer.oniceconnectionstatechange = () => {
      if (['failed', 'disconnected', 'closed'].includes(peer.iceConnectionState)) {
        clearInterval(id);
        clearTimeout(peerStats.get(peerId).peerTimeOut);
        socket.emit('peer_stats', {
            peerId: peerId,
            isDisconnected: true
        });
        peerStats.delete(peerId);
      }
    };
  }

function startCountdown(peerId) {
    const stat = peerStats.get(peerId);
    if (!stat) return;

    if (stat.peerTimeOut) {
        clearTimeout(stat.peerTimeOut);        
    }

    peerStats.set(peerId, {
        ...stat,
        peerTimeOut: setTimeout(() => {
            console.log(`Peer ${peerId} timed out`);
            cleanupPeerConnection(peerId);
        }, 10000)
    });
}

// Set up data channel event handlers
function setupDataChannel(dataChannel, peerId) {
    if (dataChannels[peerId]) return;

    dataChannel.binaryType = 'arraybuffer';

    dataChannel.onopen = () => {
        console.log(`Data channel to ${peerId} opened`);
        dataChannels[peerId] = dataChannel;

        if (!peerStats.get(peerId)) {
            peerStats.set(peerId, {
                rtt: null,
                bytesSent: 0,
                bytesReceived: 0,
            });
            startCountdown(peerId);
            onNewPeer(peerId);
        }
    };

    dataChannel.onclose = () => {
        console.log(`Data channel to ${peerId} closed`);
        delete dataChannels[peerId];
    };

    dataChannel.onerror = (error) => {
        console.error(`Data channel error with peer ${peerId}:`, error);
    };

    dataChannel.onmessage = (event) => {
        handleDataChannelMessage(event.data, peerId);
    };
}

// Handle incoming data channel messages
function handleDataChannelMessage(data, peerId) {
    try {
        startCountdown(peerId);

        if (typeof data === 'string') {
            const message = JSON.parse(data);
            console.log(`Control message from ${peerId}: ${message.type} - ${message.fragmentId}`);

            switch (message.type) {
                case 'file_request':
                    handleFileRequest(message, peerId);
                    break;
                // case 'file_info':
                //     prepareFileReception(message, peerId);
                //     break;
                case 'ready_to_receive':
                    sendFileToPeer(peerId, message.fragmentId);
                    break;
                case 'transfer_complete':
                    console.log(`Transfer of ${message.fragmentId} complete`);
                    break;
                case 'abort_transfer':
                    abortedTransfer(message.fragmentId, peerId);
                    break;
            }
        } else {
            // handleFileChunk(data, peerId);
        }
    } catch (error) {
        console.error('Error handling message:', error);
    }
}

let abortedEmitters = {};

function abortedTransfer(fragmentId, peerId) {
    if (!abortedEmitters[fragmentId]) return;

    if (abortedEmitters[fragmentId][peerId]) {
        abortedEmitters[fragmentId][peerId].emit('abort');
    }
}

// Send control message through data channel
function sendControlMessage(peerId, message) {
    startCountdown(peerId);

    if (dataChannels[peerId]?.readyState === 'open') {
        dataChannels[peerId].send(JSON.stringify(message));
    } else {
        console.error(`Cannot send message to ${peerId}: channel not open`);
    }
}

// Handle file transfer functions
function handleFileRequest(request, peerId) {
    try {
        const fragmentId = request.fragmentId;

        if (!fragmentsMap.has(fragmentId)) {
            sendControlMessage(peerId, {
                type: 'transfer_error',
                fragmentId: fragmentId,
                error: 'Fragment not found'
            });
            return;
        }

        const filePath = fragmentsMap.get(fragmentId).path;
        const stats = fs.statSync(filePath);

        sendControlMessage(peerId, {
            type: 'file_info',
            fragmentId: fragmentId,
            size: stats.size
        });
    } catch (error) {
        console.error(`Error handling file request:`, error);
    }
}

function prepareFileReception(fileInfo, peerId) {
    findSuitablePath(fileInfo.size)
        .then(suitablePath => {
            if (!suitablePath) {
                sendControlMessage(peerId, {
                    type: 'transfer_error',
                    fragmentId: fileInfo.fragmentId,
                    error: 'No suitable storage location'
                });
                return;
            }

            const filePath = path.join(suitablePath, fileInfo.fragmentId);
            activeTransfers[fileInfo.fragmentId] = {
                fileStream: fs.createWriteStream(filePath),
                filePath,
                fragmentId: fileInfo.fragmentId,
                receivedSize: 0,
                totalSize: fileInfo.size
            };

            sendControlMessage(peerId, {
                type: 'ready_to_receive',
                fragmentId: fileInfo.fragmentId
            });
        });
}

function handleFileChunk(data, peerId) {
    try {
        const view = new DataView(data);
        const fragmentIdLen = view.getUint8(0);
        const uint8Array = new Uint8Array(data);
        const fragmentId = Buffer.from(uint8Array.slice(1, 1 + fragmentIdLen)).toString();
        const fileData = uint8Array.slice(1 + fragmentIdLen);

        const transfer = activeTransfers[fragmentId];
        if (!transfer) return;

        const chunk = Buffer.from(fileData);
        transfer.fileStream.write(chunk);
        transfer.receivedSize += chunk.length;

        if (transfer.receivedSize >= transfer.totalSize) {
            transfer.fileStream.end();

            // Update fragments map
            fragmentsMap.set(transfer.fragmentId, { path: transfer.filePath });

            sendControlMessage(peerId, {
                type: 'transfer_complete',
                fragmentId: transfer.fragmentId
            });

            delete activeTransfers[fragmentId];
            sendListHashes();
        }
    } catch (error) {
        console.error('Error handling file chunk:', error);
    }
}

function sendFileToPeer(peerId, fragmentId) {
    try {
        if (!fragmentsMap.has(fragmentId)) return;

        const filePath = fragmentsMap.get(fragmentId).path;
        const dataChannel = dataChannels[peerId];

        if (!dataChannel || dataChannel.readyState !== 'open') return;

        // Check system memory before starting transfer
        si.mem().then(memInfo => {
            const availableMemoryPercent = (memInfo.available / memInfo.total) * 100;
            const fileStats = fs.statSync(filePath);
            const fileSize = fileStats.size;

            // Reject transfer if less than 15% memory available or file is too large
            if (availableMemoryPercent < 15 || dataChannel.bufferedAmount > 10 * 1024 * 1024) {
                console.log(`Rejecting file transfer: ${fragmentId} - System memory low: ${availableMemoryPercent.toFixed(2)}%`);
                socket.emit('node_overloaded', {});
                sendControlMessage(peerId, {
                    type: 'transfer_error',
                    fragmentId: fragmentId,
                    error: 'Server overloaded. Please try again later.'
                });
                return;
            }

            console.log(`Starting file transfer: ${fragmentId} to ${peerId}`);
            socket.emit('client_request', {
                fragmentId: fragmentId,
                peerId: peerId,
                start: new Date(),
                status: 'started'
            });

            // Improved abort handling
            if (!abortedEmitters[fragmentId]) abortedEmitters[fragmentId] = {};
            abortedEmitters[fragmentId][peerId] = new EventEmitter();

            // Adaptive chunk sizing based on file size
            const CHUNK_SIZE = determineOptimalChunkSize(fileSize);

            // Create read stream with optimal highWaterMark for performance
            const fileStream = fs.createReadStream(filePath, {
                highWaterMark: CHUNK_SIZE
            });

            let isTransferAborted = false;
            let pauseTimeout = null;
            let bytesTransferred = 0;
            let lastProgressReport = 0;
            const transferStartTime = Date.now();

            // Monitor transfer for performance metrics
            const statsInterval = setInterval(() => {
                if (isTransferAborted) {
                    clearInterval(statsInterval);
                    return;
                }

                const progress = Math.round((bytesTransferred / fileSize) * 100);
                const elapsedTime = (Date.now() - transferStartTime) / 1000;
                const speed = bytesTransferred / elapsedTime; // Bytes per second

                console.log(`Transfer progress: ${progress}% of ${fragmentId} to ${peerId} (${speed.toFixed(2)} B/s)`);
                socket.emit('client_request', {
                    fragmentId: fragmentId,
                    peerId: peerId,
                    status: 'progress',
                    progress: progress,
                    speed: speed.toFixed(2),
                });
            }, 5000);

            abortedEmitters[fragmentId][peerId].on('abort', () => {
                isTransferAborted = true;
                clearInterval(statsInterval);
                fileStream.destroy(); // Immediately clean up resources
            });

            // Create a buffer pool for reuse to reduce GC pressure
            const bufferCache = Buffer.allocUnsafe(CHUNK_SIZE);

            fileStream.on('data', (chunk) => {
                if (isTransferAborted) return;

                // Track transfer progress
                bytesTransferred += chunk.length;

                // Check if this is the last chunk
                const isLastChunk = bytesTransferred >= fileSize;
                if (isLastChunk) {
                    //console.log(`Sending final chunk of ${fragmentId} to ${peerId}`);
                }

                // Prepare data packet with efficient buffer concatenation
                const fragmentIdBuffer = Buffer.from(fragmentId);
                const headerBuffer = Buffer.alloc(2); // 1 byte for ID length, 1 byte for lastChunk flag
                headerBuffer.writeUInt8(fragmentIdBuffer.length, 0);
                headerBuffer.writeUInt8(isLastChunk ? 1 : 0, 1); // Add last chunk flag

                // Efficient buffer allocation
                const data = Buffer.concat([headerBuffer, fragmentIdBuffer, chunk],
                    2 + fragmentIdBuffer.length + chunk.length);

                // Improved backpressure handling with dynamic throttling
                if (dataChannel.bufferedAmount > CHUNK_SIZE * 8) {
                    fileStream.pause();

                    if (pauseTimeout) clearTimeout(pauseTimeout);

                    // Dynamic timeout based on buffer fullness
                    const timeoutDuration = Math.min(
                        Math.max(1000, Math.floor(dataChannel.bufferedAmount / 1024)),
                        15000
                    );

                    pauseTimeout = setTimeout(() => {
                        // Progressive recovery strategy
                        if (dataChannel.bufferedAmount > CHUNK_SIZE * 4) {
                            console.log(`Buffer still high (${(dataChannel.bufferedAmount / 1024 / 1024).toFixed(2)}MB), extending pause...`);
                            // Reduce chunk size temporarily if congestion persists
                            if (pauseTimeout) clearTimeout(pauseTimeout);
                            pauseTimeout = setTimeout(() => resumeOrAbort(), 5000);
                        } else {
                            resumeOrAbort();
                        }
                    }, timeoutDuration);

                    // Efficient buffer monitoring with binary backoff
                    let backoffTime = 50;
                    const bufferInterval = setInterval(() => {
                        if (isTransferAborted) {
                            clearInterval(bufferInterval);
                            return;
                        }

                        if (dataChannel.bufferedAmount < CHUNK_SIZE) {
                            clearInterval(bufferInterval);
                            if (pauseTimeout) {
                                clearTimeout(pauseTimeout);
                                pauseTimeout = null;
                            }
                            fileStream.resume();
                        } else {
                            // Exponential backoff for buffer checking
                            backoffTime = Math.min(backoffTime * 1.5, 1000);
                            setTimeout(() => {
                                if (!isTransferAborted) fileStream.resume();
                            }, backoffTime);
                        }
                    }, backoffTime);
                }

                // Send the data
                try {
                    dataChannel.send(data);
                } catch (err) {
                    console.error(`Error sending data chunk: ${err.message}`);
                    isTransferAborted = true;
                    fileStream.destroy();
                }
            });

            function resumeOrAbort() {
                if (dataChannel.bufferedAmount > 1024 * 1024 * 5) {  // 5MB threshold
                    console.log(`Aborting transfer due to persistent network congestion`);
                    isTransferAborted = true;
                    fileStream.destroy();
                    socket.emit('client_request', {
                        fragmentId: fragmentId,
                        peerId: peerId,
                        status: 'error',
                        error: 'Network congestion',
                        end: new Date()
                    });
                    sendControlMessage(peerId, {
                        type: 'transfer_error',
                        fragmentId: fragmentId,
                        error: 'Transfer aborted due to network congestion'
                    });
                } else {
                    fileStream.resume();
                }
            }

            fileStream.on('error', (error) => {
                clearInterval(statsInterval);
                console.error(`Error reading file during transfer: ${error.message}`);
                sendControlMessage(peerId, {
                    type: 'transfer_error',
                    fragmentId: fragmentId,
                    error: 'File reading error'
                });
                socket.emit('client_request', {
                    fragmentId: fragmentId,
                    peerId: peerId,
                    status: 'error',
                    error: 'File reading error',
                    end: new Date()
                });
            });

            fileStream.on('end', () => {
                clearInterval(statsInterval);
                if (!isTransferAborted) {
                    const totalTime = (Date.now() - transferStartTime) / 1000;
                    const avgSpeed = (fileSize / 1024 / totalTime).toFixed(2);
                    console.log(`Completed sending file ${fragmentId} to ${peerId} in ${totalTime.toFixed(1)}s (avg: ${avgSpeed} KB/s)`);

                    sendControlMessage(peerId, {
                        type: 'transfer_complete',
                        fragmentId: fragmentId
                    });

                    socket.emit('client_request', {
                        fragmentId: fragmentId,
                        peerId: peerId,
                        status: 'complete',
                        avgSpeed: avgSpeed,
                        end: new Date()
                    });
                } else {
                    console.log(`Transfer of ${fragmentId} to ${peerId} was aborted`);
                    socket.emit('client_request', {
                        fragmentId: fragmentId,
                        peerId: peerId,
                        status: 'aborted',
                        end: new Date()
                    });
                }
                delete abortedEmitters[fragmentId][peerId];
            });
        }).catch(error => {
            console.error('Error checking memory:', error);
            sendControlMessage(peerId, {
                type: 'transfer_error',
                fragmentId: fragmentId,
                error: 'Unable to check system resources'
            });
            socket.emit('client_request', {
                fragmentId: fragmentId,
                peerId: peerId,
                status: 'error',
                error: 'Unable to check system resources',
                end: new Date()
            });
        });
    } catch (error) {
        console.error(`Error sending file:`, error);
        if (dataChannels[peerId]?.readyState === 'open') {
            sendControlMessage(peerId, {
                type: 'transfer_error',
                fragmentId: fragmentId,
                error: 'Internal server error'
            });
        }
    }
}

// Helper function to determine optimal chunk size based on file size
function determineOptimalChunkSize(fileSize) {
    // For very small files, use smaller chunks
    if (fileSize < 100 * 1024) return 4 * 1024;  // 4KB for files under 100KB

    // For small files, use medium chunks
    if (fileSize < 1024 * 1024) return 16 * 1024;  // 16KB for files under 1MB

    // For medium files
    if (fileSize < 10 * 1024 * 1024) return 32 * 1024;  // 32KB for files under 10MB

    // For large files
    if (fileSize < 100 * 1024 * 1024) return 64 * 1024;  // 64KB for files under 100MB

    // For very large files
    return 128 * 1024;  // 128KB for files over 100MB
}

function cleanupPeerConnection(peerId) {
    if (dataChannels[peerId]) {
        dataChannels[peerId].close();
        delete dataChannels[peerId];
    }

    if (peerConnections[peerId]) {
        peerConnections[peerId].close();
        delete peerConnections[peerId];
    }
}

// WebRTC signaling handlers
socket.on('offer', async (data) => {
    try {
        const peerId = data.sender;
        if (peerConnections[peerId]) cleanupPeerConnection(peerId);

        const peerConnection = createPeerConnection(peerId);

        peerConnection.ondatachannel = (event) => {
            setupDataChannel(event.channel, peerId);
        };

        await peerConnection.setRemoteDescription(new RTCSessionDescription(data.offer));
        const answer = await peerConnection.createAnswer();
        await peerConnection.setLocalDescription(answer);

        socket.emit('answer', {
            target: peerId,
            answer: answer
        });
    } catch (error) {
        console.error('Error handling WebRTC offer:', error);
    }
});

socket.on('answer', async (data) => {
    try {
        const peerConnection = peerConnections[data.sender];
        if (peerConnection) {
            await peerConnection.setRemoteDescription(new RTCSessionDescription(data.answer));
        }
    } catch (error) {
        console.error('Error handling WebRTC answer:', error);
    }
});

socket.on('ice_candidate', async (data) => {
    try {
        const peerConnection = peerConnections[data.sender];
        if (peerConnection) {
            await peerConnection.addIceCandidate(new RTCIceCandidate(data.candidate));
        }
    } catch (error) {
        console.error('Error adding ICE candidate:', error);
    }
});

// Initialize WebRTC when connected
socket.on('connect', () => {
    console.log('WebRTC initialized with config:', webrtcConfig);
});

// Request fragment from peer
function requestFragmentFromPeer(peerId, fragmentId) {
    if (!peerConnections[peerId]) {
        const peerConnection = createPeerConnection(peerId);
        peerConnection.createOffer()
            .then(offer => peerConnection.setLocalDescription(offer))
            .then(() => {
                socket.emit('offer', {
                    target: peerId,
                    offer: peerConnection.localDescription,
                });
            });
    } else if (dataChannels[peerId]?.readyState === 'open') {
        sendControlMessage(peerId, {
            type: 'file_request',
            fragmentId: fragmentId
        });
    }
}