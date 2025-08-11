"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.FumaroleSM = exports.SlotDownloadState = exports.SlotDownloadProgress = exports.SlotCommitmentProgression = exports.FumeSlotStatus = exports.FumeDownloadRequest = exports.CommitmentLevel = exports.DEFAULT_SLOT_MEMORY_RETENTION = void 0;
var queue_1 = require("./queue");
// Constants
exports.DEFAULT_SLOT_MEMORY_RETENTION = 10000;
// Solana commitment levels
var CommitmentLevel;
(function (CommitmentLevel) {
    CommitmentLevel[CommitmentLevel["PROCESSED"] = 0] = "PROCESSED";
    CommitmentLevel[CommitmentLevel["CONFIRMED"] = 1] = "CONFIRMED";
    CommitmentLevel[CommitmentLevel["FINALIZED"] = 2] = "FINALIZED";
})(CommitmentLevel || (exports.CommitmentLevel = CommitmentLevel = {}));
// Data structures
var FumeDownloadRequest = /** @class */ (function () {
    function FumeDownloadRequest(slot, blockchainId, blockUid, numShards, commitmentLevel) {
        this.slot = slot;
        this.blockchainId = blockchainId;
        this.blockUid = blockUid;
        this.numShards = numShards;
        this.commitmentLevel = commitmentLevel;
    }
    return FumeDownloadRequest;
}());
exports.FumeDownloadRequest = FumeDownloadRequest;
var FumeSlotStatus = /** @class */ (function () {
    function FumeSlotStatus(sessionSequence, offset, slot, parentSlot, commitmentLevel, deadError) {
        this.sessionSequence = sessionSequence;
        this.offset = offset;
        this.slot = slot;
        this.parentSlot = parentSlot;
        this.commitmentLevel = commitmentLevel;
        this.deadError = deadError;
    }
    return FumeSlotStatus;
}());
exports.FumeSlotStatus = FumeSlotStatus;
var SlotCommitmentProgression = /** @class */ (function () {
    function SlotCommitmentProgression() {
        this.processedCommitmentLevels = new Set();
    }
    SlotCommitmentProgression.prototype.hasProcessedCommitment = function (level) {
        return this.processedCommitmentLevels.has(level);
    };
    SlotCommitmentProgression.prototype.addProcessedCommitment = function (level) {
        this.processedCommitmentLevels.add(level);
    };
    return SlotCommitmentProgression;
}());
exports.SlotCommitmentProgression = SlotCommitmentProgression;
var SlotDownloadProgress = /** @class */ (function () {
    function SlotDownloadProgress(numShards) {
        this.numShards = numShards;
        this.shardRemaining = new Array(numShards).fill(false);
    }
    SlotDownloadProgress.prototype.doProgress = function (shardIdx) {
        this.shardRemaining[shardIdx % this.numShards] = true;
        return this.shardRemaining.every(function (x) { return x; })
            ? SlotDownloadState.Done
            : SlotDownloadState.Downloading;
    };
    return SlotDownloadProgress;
}());
exports.SlotDownloadProgress = SlotDownloadProgress;
var SlotDownloadState;
(function (SlotDownloadState) {
    SlotDownloadState["Downloading"] = "Downloading";
    SlotDownloadState["Done"] = "Done";
})(SlotDownloadState || (exports.SlotDownloadState = SlotDownloadState = {}));
var FumaroleSM = /** @class */ (function () {
    function FumaroleSM(lastCommittedOffset, slotMemoryRetention) {
        this.slotMemoryRetention = slotMemoryRetention;
        this.slotCommitmentProgression = new Map();
        this.downloadedSlot = new Set();
        this.inflightSlotShardDownload = new Map();
        this.blockedSlotStatusUpdate = new Map();
        this.slotStatusUpdateQueue = new queue_1.Queue();
        this.processedOffset = []; // Min-heap for (sequence, offset)
        this.maxSlotDetected = 0;
        this.unprocessedBlockchainEvent = new queue_1.Queue();
        this.sequence = 1;
        this.lastProcessedFumeSequence = 0;
        this.sequenceToOffset = new Map();
        this._lastCommittedOffset = lastCommittedOffset;
        this._committableOffset = lastCommittedOffset;
    }
    Object.defineProperty(FumaroleSM.prototype, "lastCommittedOffset", {
        get: function () {
            return this._lastCommittedOffset;
        },
        enumerable: false,
        configurable: true
    });
    Object.defineProperty(FumaroleSM.prototype, "committableOffset", {
        get: function () {
            return this._committableOffset;
        },
        enumerable: false,
        configurable: true
    });
    FumaroleSM.prototype.updateCommittedOffset = function (offset) {
        if (BigInt(offset) < BigInt(this._lastCommittedOffset)) {
            throw new Error("Offset must be >= last committed offset");
        }
        this._lastCommittedOffset = offset;
    };
    FumaroleSM.prototype.nextSequence = function () {
        var ret = this.sequence;
        this.sequence += 1;
        return ret;
    };
    FumaroleSM.prototype.gc = function () {
        while (this.downloadedSlot.size > this.slotMemoryRetention) {
            // Get the first slot (oldest) from the set
            var slot = this.downloadedSlot.values().next().value;
            if (!slot)
                break;
            this.downloadedSlot.delete(slot);
            this.slotCommitmentProgression.delete(slot);
            this.inflightSlotShardDownload.delete(slot);
            this.blockedSlotStatusUpdate.delete(slot);
        }
    };
    FumaroleSM.prototype.queueBlockchainEvent = function (events) {
        return __awaiter(this, void 0, void 0, function () {
            var _i, events_1, event_1, sequence, fumeStatus, blockedQueue;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        _i = 0, events_1 = events;
                        _a.label = 1;
                    case 1:
                        if (!(_i < events_1.length)) return [3 /*break*/, 9];
                        event_1 = events_1[_i];
                        if (BigInt(event_1.offset) < BigInt(this._lastCommittedOffset)) {
                            return [3 /*break*/, 8];
                        }
                        if (event_1.slot > this.maxSlotDetected) {
                            this.maxSlotDetected = event_1.slot;
                        }
                        sequence = this.nextSequence();
                        this.sequenceToOffset.set(sequence, event_1.offset);
                        if (!this.downloadedSlot.has(event_1.slot)) return [3 /*break*/, 6];
                        fumeStatus = new FumeSlotStatus(sequence, event_1.offset, event_1.slot, event_1.parentSlot, event_1.commitmentLevel, event_1.deadError);
                        if (!this.inflightSlotShardDownload.has(event_1.slot)) return [3 /*break*/, 3];
                        blockedQueue = this.blockedSlotStatusUpdate.get(event_1.slot);
                        if (!blockedQueue) {
                            blockedQueue = new queue_1.Queue();
                            this.blockedSlotStatusUpdate.set(event_1.slot, blockedQueue);
                        }
                        return [4 /*yield*/, blockedQueue.put(fumeStatus)];
                    case 2:
                        _a.sent();
                        return [3 /*break*/, 5];
                    case 3: return [4 /*yield*/, this.slotStatusUpdateQueue.put(fumeStatus)];
                    case 4:
                        _a.sent();
                        _a.label = 5;
                    case 5: return [3 /*break*/, 8];
                    case 6: return [4 /*yield*/, this.unprocessedBlockchainEvent.put([sequence, event_1])];
                    case 7:
                        _a.sent();
                        _a.label = 8;
                    case 8:
                        _i++;
                        return [3 /*break*/, 1];
                    case 9: return [2 /*return*/];
                }
            });
        });
    };
    FumaroleSM.prototype.makeSlotDownloadProgress = function (slot, shardIdx) {
        return __awaiter(this, void 0, void 0, function () {
            var downloadProgress, downloadState, blockedStatuses, status_1;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        downloadProgress = this.inflightSlotShardDownload.get(slot);
                        if (!downloadProgress) {
                            throw new Error("Slot not in download");
                        }
                        downloadState = downloadProgress.doProgress(shardIdx);
                        if (!(downloadState === SlotDownloadState.Done)) return [3 /*break*/, 6];
                        this.inflightSlotShardDownload.delete(slot);
                        this.downloadedSlot.add(slot);
                        if (!this.slotCommitmentProgression.has(slot)) {
                            this.slotCommitmentProgression.set(slot, new SlotCommitmentProgression());
                        }
                        blockedStatuses = this.blockedSlotStatusUpdate.get(slot);
                        if (!blockedStatuses) return [3 /*break*/, 6];
                        _a.label = 1;
                    case 1:
                        if (!!blockedStatuses.isEmpty()) return [3 /*break*/, 5];
                        return [4 /*yield*/, blockedStatuses.get()];
                    case 2:
                        status_1 = _a.sent();
                        if (!status_1) return [3 /*break*/, 4];
                        return [4 /*yield*/, this.slotStatusUpdateQueue.put(status_1)];
                    case 3:
                        _a.sent();
                        _a.label = 4;
                    case 4: return [3 /*break*/, 1];
                    case 5:
                        this.blockedSlotStatusUpdate.delete(slot);
                        _a.label = 6;
                    case 6: return [2 /*return*/, downloadState];
                }
            });
        });
    };
    FumaroleSM.prototype.popNextSlotStatus = function () {
        return __awaiter(this, void 0, void 0, function () {
            var slotStatus, commitmentHistory;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!!this.slotStatusUpdateQueue.isEmpty()) return [3 /*break*/, 2];
                        return [4 /*yield*/, this.slotStatusUpdateQueue.get()];
                    case 1:
                        slotStatus = _a.sent();
                        if (!slotStatus)
                            return [3 /*break*/, 0];
                        commitmentHistory = this.slotCommitmentProgression.get(slotStatus.slot);
                        if (commitmentHistory &&
                            !commitmentHistory.hasProcessedCommitment(slotStatus.commitmentLevel)) {
                            commitmentHistory.addProcessedCommitment(slotStatus.commitmentLevel);
                            return [2 /*return*/, slotStatus];
                        }
                        else if (!commitmentHistory) {
                            throw new Error("Slot status should not be available here");
                        }
                        return [3 /*break*/, 0];
                    case 2: return [2 /*return*/, null];
                }
            });
        });
    };
    FumaroleSM.prototype.makeSureSlotCommitmentProgressionExists = function (slot) {
        var progression = this.slotCommitmentProgression.get(slot);
        if (!progression) {
            progression = new SlotCommitmentProgression();
            this.slotCommitmentProgression.set(slot, progression);
        }
        return progression;
    };
    FumaroleSM.prototype.popSlotToDownload = function () {
        return __awaiter(this, arguments, void 0, function (commitment) {
            var eventPair, sessionSequence, blockchainEvent, eventCl, progression, blockchainId, blockUid, downloadRequest, downloadProgress, blockedQueue;
            if (commitment === void 0) { commitment = CommitmentLevel.PROCESSED; }
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!!this.unprocessedBlockchainEvent.isEmpty()) return [3 /*break*/, 8];
                        return [4 /*yield*/, this.unprocessedBlockchainEvent.get()];
                    case 1:
                        eventPair = _a.sent();
                        if (!eventPair)
                            return [3 /*break*/, 0];
                        sessionSequence = eventPair[0], blockchainEvent = eventPair[1];
                        eventCl = blockchainEvent.commitmentLevel;
                        if (!(eventCl < commitment)) return [3 /*break*/, 3];
                        return [4 /*yield*/, this.slotStatusUpdateQueue.put(new FumeSlotStatus(sessionSequence, blockchainEvent.offset, blockchainEvent.slot, blockchainEvent.parentSlot, eventCl, blockchainEvent.deadError))];
                    case 2:
                        _a.sent();
                        this.makeSureSlotCommitmentProgressionExists(blockchainEvent.slot);
                        return [3 /*break*/, 0];
                    case 3:
                        if (!this.downloadedSlot.has(blockchainEvent.slot)) return [3 /*break*/, 5];
                        this.makeSureSlotCommitmentProgressionExists(blockchainEvent.slot);
                        progression = this.slotCommitmentProgression.get(blockchainEvent.slot);
                        if (progression && progression.hasProcessedCommitment(eventCl)) {
                            this.markEventAsProcessed(sessionSequence);
                            return [3 /*break*/, 0];
                        }
                        return [4 /*yield*/, this.slotStatusUpdateQueue.put(new FumeSlotStatus(sessionSequence, blockchainEvent.offset, blockchainEvent.slot, blockchainEvent.parentSlot, eventCl, blockchainEvent.deadError))];
                    case 4:
                        _a.sent();
                        return [3 /*break*/, 7];
                    case 5:
                        blockchainId = new Uint8Array(blockchainEvent.blockchainId);
                        blockUid = new Uint8Array(blockchainEvent.blockUid);
                        if (!!this.inflightSlotShardDownload.has(blockchainEvent.slot)) return [3 /*break*/, 7];
                        downloadRequest = new FumeDownloadRequest(blockchainEvent.slot, blockchainId, blockUid, blockchainEvent.numShards, eventCl);
                        downloadProgress = new SlotDownloadProgress(blockchainEvent.numShards);
                        this.inflightSlotShardDownload.set(blockchainEvent.slot, downloadProgress);
                        blockedQueue = this.blockedSlotStatusUpdate.get(blockchainEvent.slot);
                        if (!blockedQueue) {
                            blockedQueue = new queue_1.Queue();
                            this.blockedSlotStatusUpdate.set(blockchainEvent.slot, blockedQueue);
                        }
                        return [4 /*yield*/, blockedQueue.put(new FumeSlotStatus(sessionSequence, blockchainEvent.offset, blockchainEvent.slot, blockchainEvent.parentSlot, eventCl, blockchainEvent.deadError))];
                    case 6:
                        _a.sent();
                        return [2 /*return*/, downloadRequest];
                    case 7: return [3 /*break*/, 0];
                    case 8: return [2 /*return*/, null];
                }
            });
        });
    };
    FumaroleSM.prototype.markEventAsProcessed = function (eventSeqNumber) {
        var fumeOffset = this.sequenceToOffset.get(eventSeqNumber);
        if (!fumeOffset) {
            throw new Error("Event sequence number not found");
        }
        this.sequenceToOffset.delete(eventSeqNumber);
        // Use negative values for the min-heap (to simulate max-heap behavior)
        this.processedOffset.push([-eventSeqNumber, fumeOffset]);
        this.processedOffset.sort(function (a, b) { return a[0] - b[0]; }); // Keep sorted as a min-heap
        while (this.processedOffset.length > 0) {
            var _a = this.processedOffset[0], seq = _a[0], offset = _a[1];
            var positiveSeq = -seq; // Convert back to positive
            if (positiveSeq !== this.lastProcessedFumeSequence + 1) {
                break;
            }
            this.processedOffset.shift();
            this._committableOffset = offset;
            this.lastProcessedFumeSequence = positiveSeq;
        }
    };
    FumaroleSM.prototype.slotStatusUpdateQueueLen = function () {
        return this.slotStatusUpdateQueue.size();
    };
    FumaroleSM.prototype.processedOffsetQueueLen = function () {
        return this.processedOffset.length;
    };
    FumaroleSM.prototype.needNewBlockchainEvents = function () {
        return (this.slotStatusUpdateQueue.isEmpty() &&
            this.blockedSlotStatusUpdate.size === 0);
    };
    return FumaroleSM;
}());
exports.FumaroleSM = FumaroleSM;
