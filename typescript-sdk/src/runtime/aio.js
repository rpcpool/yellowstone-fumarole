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
exports.GrpcDownloadBlockTaskRun = exports.GrpcSlotDownloader = exports.AsyncioFumeDragonsmouthRuntime = exports.DEFAULT_SLOT_MEMORY_RETENTION = exports.DEFAULT_GC_INTERVAL = void 0;
var grpc_js_1 = require("@grpc/grpc-js");
var aio_1 = require("../utils/aio");
// Constants
exports.DEFAULT_GC_INTERVAL = 5;
exports.DEFAULT_SLOT_MEMORY_RETENTION = 10000;
var LOGGER = console;
var AsyncioFumeDragonsmouthRuntime = /** @class */ (function () {
    function AsyncioFumeDragonsmouthRuntime(sm, slotDownloader, subscribeRequestUpdateQ, subscribeRequest, consumerGroupName, controlPlaneTxQ, controlPlaneRxQ, dragonsmouthOutlet, commitInterval, gcInterval, maxConcurrentDownload) {
        if (maxConcurrentDownload === void 0) { maxConcurrentDownload = 10; }
        this.sm = sm;
        this.slotDownloader = slotDownloader;
        this.subscribeRequestUpdateQ = subscribeRequestUpdateQ;
        this.subscribeRequest = subscribeRequest;
        this.consumerGroupName = consumerGroupName;
        this.controlPlaneTx = controlPlaneTxQ;
        this.controlPlaneRx = controlPlaneRxQ;
        this.dragonsmouthOutlet = dragonsmouthOutlet;
        this.commitInterval = commitInterval;
        this.gcInterval = gcInterval;
        this.maxConcurrentDownload = maxConcurrentDownload;
        this.downloadTasks = new Map();
        this.lastCommit = Date.now();
    }
    AsyncioFumeDragonsmouthRuntime.prototype.buildPollHistoryCmd = function (fromOffset) {
        return { pollHist: { shardId: 0 } };
    };
    AsyncioFumeDragonsmouthRuntime.prototype.buildCommitOffsetCmd = function (offset) {
        return { commitOffset: { offset: offset, shardId: 0 } };
    };
    AsyncioFumeDragonsmouthRuntime.prototype.handleControlResponse = function (controlResponse) {
        var _a;
        // Get first defined property from controlResponse
        var responseField = Object.keys(controlResponse).find(function (key) { return controlResponse[key] !== undefined && key !== "response"; });
        if (!responseField) {
            throw new Error("Control response is empty");
        }
        switch (responseField) {
            case "pollHist": {
                var pollHist = controlResponse.pollHist;
                LOGGER.debug("Received poll history ".concat((_a = pollHist.events) === null || _a === void 0 ? void 0 : _a.length, " events"));
                // Convert string slots to numbers and map commitment levels
                var convertedEvents = (pollHist.events || []).map(function (event) { return ({
                    offset: event.offset,
                    slot: Number(event.slot),
                    parentSlot: event.parentSlot ? Number(event.parentSlot) : undefined,
                    commitmentLevel: event.commitmentLevel,
                    deadError: event.deadError,
                    blockchainId: event.blockchainId,
                    blockUid: event.blockUid,
                    numShards: Number(event.numShards),
                }); });
                this.sm.queueBlockchainEvent(convertedEvents);
                break;
            }
            case "commitOffset": {
                var commitOffset = controlResponse.commitOffset;
                LOGGER.debug("Received commit offset: ".concat(commitOffset));
                this.sm.updateCommittedOffset(commitOffset.offset);
                break;
            }
            case "pong":
                LOGGER.debug("Received pong");
                break;
            default:
                throw new Error("Unexpected control response");
        }
    };
    AsyncioFumeDragonsmouthRuntime.prototype.pollHistoryIfNeeded = function () {
        return __awaiter(this, void 0, void 0, function () {
            var cmd;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!this.sm.needNewBlockchainEvents()) return [3 /*break*/, 2];
                        cmd = this.buildPollHistoryCmd(this.sm.committableOffset);
                        return [4 /*yield*/, this.controlPlaneTx.put(cmd)];
                    case 1:
                        _a.sent();
                        _a.label = 2;
                    case 2: return [2 /*return*/];
                }
            });
        });
    };
    AsyncioFumeDragonsmouthRuntime.prototype.commitmentLevel = function () {
        return this.subscribeRequest.commitment || 0;
    };
    AsyncioFumeDragonsmouthRuntime.prototype.scheduleDownloadTaskIfAny = function () {
        return __awaiter(this, void 0, void 0, function () {
            var downloadRequest, downloadTaskArgs, downloadPromise;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!true) return [3 /*break*/, 2];
                        LOGGER.debug("Checking for download tasks to schedule");
                        if (this.downloadTasks.size >= this.maxConcurrentDownload) {
                            return [3 /*break*/, 2];
                        }
                        LOGGER.debug("Popping slot to download");
                        return [4 /*yield*/, this.sm.popSlotToDownload(this.commitmentLevel())];
                    case 1:
                        downloadRequest = _a.sent();
                        if (!downloadRequest) {
                            LOGGER.debug("No download request available");
                            return [3 /*break*/, 2];
                        }
                        LOGGER.debug("Download request for slot ".concat(downloadRequest.slot, " popped"));
                        if (!downloadRequest.blockchainId) {
                            throw new Error("Download request must have a blockchain ID");
                        }
                        downloadTaskArgs = {
                            downloadRequest: downloadRequest,
                            dragonsmouthOutlet: this.dragonsmouthOutlet,
                        };
                        downloadPromise = this.slotDownloader.runDownload(this.subscribeRequest, downloadTaskArgs);
                        this.downloadTasks.set(downloadPromise, downloadRequest);
                        LOGGER.debug("Scheduling download task for slot ".concat(downloadRequest.slot));
                        return [3 /*break*/, 0];
                    case 2: return [2 /*return*/];
                }
            });
        });
    };
    AsyncioFumeDragonsmouthRuntime.prototype.handleDownloadResult = function (downloadResult) {
        if (downloadResult.kind === "Ok") {
            var completed = downloadResult.completed;
            LOGGER.debug("Download completed for slot ".concat(completed.slot, ", shard ").concat(completed.shardIdx, ", ").concat(completed.totalEventDownloaded, " total events"));
            this.sm.makeSlotDownloadProgress(completed.slot, completed.shardIdx);
        }
        else {
            var slot = downloadResult.slot;
            var err = downloadResult.err;
            throw new Error("Failed to download slot ".concat(slot, ": ").concat(err.message));
        }
    };
    AsyncioFumeDragonsmouthRuntime.prototype.forceCommitOffset = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        LOGGER.debug("Force committing offset ".concat(this.sm.committableOffset));
                        return [4 /*yield*/, this.controlPlaneTx.put(this.buildCommitOffsetCmd(this.sm.committableOffset))];
                    case 1:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    AsyncioFumeDragonsmouthRuntime.prototype.commitOffset = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!(this.sm.lastCommittedOffset < this.sm.committableOffset)) return [3 /*break*/, 2];
                        LOGGER.debug("Committing offset ".concat(this.sm.committableOffset));
                        return [4 /*yield*/, this.forceCommitOffset()];
                    case 1:
                        _a.sent();
                        _a.label = 2;
                    case 2:
                        this.lastCommit = Date.now();
                        return [2 /*return*/];
                }
            });
        });
    };
    AsyncioFumeDragonsmouthRuntime.prototype.drainSlotStatus = function () {
        return __awaiter(this, void 0, void 0, function () {
            var commitment, slotStatusVec, slotStatus, _i, slotStatusVec_1, slotStatus, matchedFilters, _a, _b, _c, filterName, filter, update, error_1;
            return __generator(this, function (_d) {
                switch (_d.label) {
                    case 0:
                        commitment = this.subscribeRequest.commitment || 0;
                        slotStatusVec = [];
                        while (true) {
                            slotStatus = this.sm.popNextSlotStatus();
                            if (!slotStatus)
                                break;
                            slotStatusVec.push(slotStatus);
                        }
                        if (!slotStatusVec.length)
                            return [2 /*return*/];
                        LOGGER.debug("Draining ".concat(slotStatusVec.length, " slot status"));
                        _i = 0, slotStatusVec_1 = slotStatusVec;
                        _d.label = 1;
                    case 1:
                        if (!(_i < slotStatusVec_1.length)) return [3 /*break*/, 7];
                        slotStatus = slotStatusVec_1[_i];
                        matchedFilters = [];
                        for (_a = 0, _b = Object.entries(this.subscribeRequest.slots || {}); _a < _b.length; _a++) {
                            _c = _b[_a], filterName = _c[0], filter = _c[1];
                            if (filter.filterByCommitment &&
                                slotStatus.commitmentLevel === commitment) {
                                matchedFilters.push(filterName);
                            }
                            else if (!filter.filterByCommitment) {
                                matchedFilters.push(filterName);
                            }
                        }
                        if (!matchedFilters.length) return [3 /*break*/, 5];
                        update = {
                            filters: matchedFilters,
                            createdAt: undefined,
                            slot: {
                                slot: slotStatus.slot,
                                parent: slotStatus.parentSlot,
                                status: slotStatus.commitmentLevel,
                                deadError: slotStatus.deadError,
                            },
                        };
                        _d.label = 2;
                    case 2:
                        _d.trys.push([2, 4, , 5]);
                        return [4 /*yield*/, this.dragonsmouthOutlet.put(update)];
                    case 3:
                        _d.sent();
                        return [3 /*break*/, 5];
                    case 4:
                        error_1 = _d.sent();
                        if (error_1.message === "Queue full")
                            return [2 /*return*/];
                        throw error_1;
                    case 5:
                        this.sm.markEventAsProcessed(slotStatus.sessionSequence);
                        _d.label = 6;
                    case 6:
                        _i++;
                        return [3 /*break*/, 1];
                    case 7: return [2 /*return*/];
                }
            });
        });
    };
    AsyncioFumeDragonsmouthRuntime.prototype.handleControlPlaneResp = function (result) {
        return __awaiter(this, void 0, void 0, function () {
            var errorUpdate;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (!(result instanceof Error)) return [3 /*break*/, 2];
                        errorUpdate = {
                            filters: [],
                            createdAt: undefined,
                            slot: {
                                slot: "0",
                                parent: "0",
                                status: 0, // Using 0 as default status for error case
                                deadError: result.message,
                            },
                        };
                        return [4 /*yield*/, this.dragonsmouthOutlet.put(errorUpdate)];
                    case 1:
                        _a.sent();
                        LOGGER.error("Control plane error: ".concat(result.message));
                        return [2 /*return*/, false];
                    case 2:
                        this.handleControlResponse(result);
                        return [2 /*return*/, true];
                }
            });
        });
    };
    AsyncioFumeDragonsmouthRuntime.prototype.handleNewSubscribeRequest = function (subscribeRequest) {
        this.subscribeRequest = subscribeRequest;
    };
    AsyncioFumeDragonsmouthRuntime.prototype.run = function () {
        return __awaiter(this, void 0, void 0, function () {
            var ticks, taskMap, downloadTasks, _i, downloadTasks_1, task, downloadTaskInFlight, promises, done, taskName, _a, result, newTask, newTask, newTask;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        LOGGER.debug("Fumarole runtime starting...");
                        return [4 /*yield*/, this.controlPlaneTx.put(this.buildPollHistoryCmd())];
                    case 1:
                        _b.sent();
                        LOGGER.debug("Initial poll history command sent");
                        return [4 /*yield*/, this.forceCommitOffset()];
                    case 2:
                        _b.sent();
                        LOGGER.debug("Initial commit offset command sent");
                        ticks = 0;
                        taskMap = new Map();
                        // Initial tasks
                        taskMap.set(this.subscribeRequestUpdateQ.get(), "dragonsmouth_bidi");
                        taskMap.set(this.controlPlaneRx.get(), "control_plane_rx");
                        taskMap.set(new aio_1.Interval(this.commitInterval).tick(), "commit_tick");
                        _b.label = 3;
                    case 3:
                        if (!(taskMap.size > 0)) return [3 /*break*/, 16];
                        ticks++;
                        LOGGER.debug("Runtime loop tick");
                        if (ticks % this.gcInterval === 0) {
                            LOGGER.debug("Running garbage collection");
                            this.sm.gc();
                            ticks = 0;
                        }
                        LOGGER.debug("Polling history if needed");
                        return [4 /*yield*/, this.pollHistoryIfNeeded()];
                    case 4:
                        _b.sent();
                        LOGGER.debug("Scheduling download tasks if any");
                        return [4 /*yield*/, this.scheduleDownloadTaskIfAny()];
                    case 5:
                        _b.sent();
                        downloadTasks = Array.from(this.downloadTasks.keys());
                        for (_i = 0, downloadTasks_1 = downloadTasks; _i < downloadTasks_1.length; _i++) {
                            task = downloadTasks_1[_i];
                            taskMap.set(task, "download_task");
                        }
                        downloadTaskInFlight = this.downloadTasks.size;
                        LOGGER.debug("Current download tasks in flight: ".concat(downloadTaskInFlight, " / ").concat(this.maxConcurrentDownload));
                        promises = Array.from(taskMap.keys());
                        return [4 /*yield*/, Promise.race(promises.map(function (p) { return p.then(function (result) { return ({ promise: p, result: result }); }); }))];
                    case 6:
                        done = _b.sent();
                        taskName = taskMap.get(done.promise);
                        taskMap.delete(done.promise);
                        _a = taskName;
                        switch (_a) {
                            case "dragonsmouth_bidi": return [3 /*break*/, 7];
                            case "control_plane_rx": return [3 /*break*/, 8];
                            case "download_task": return [3 /*break*/, 10];
                            case "commit_tick": return [3 /*break*/, 11];
                        }
                        return [3 /*break*/, 13];
                    case 7:
                        {
                            LOGGER.debug("Dragonsmouth subscribe request received");
                            result = done.result;
                            this.handleNewSubscribeRequest(result);
                            newTask = this.subscribeRequestUpdateQ.get();
                            taskMap.set(newTask, "dragonsmouth_bidi");
                            return [3 /*break*/, 14];
                        }
                        _b.label = 8;
                    case 8:
                        LOGGER.debug("Control plane response received");
                        return [4 /*yield*/, this.handleControlPlaneResp(done.result)];
                    case 9:
                        if (!(_b.sent())) {
                            LOGGER.debug("Control plane error");
                            return [2 /*return*/];
                        }
                        newTask = this.controlPlaneRx.get();
                        taskMap.set(newTask, "control_plane_rx");
                        return [3 /*break*/, 14];
                    case 10:
                        {
                            LOGGER.debug("Download task result received");
                            this.downloadTasks.delete(done.promise);
                            this.handleDownloadResult(done.result);
                            return [3 /*break*/, 14];
                        }
                        _b.label = 11;
                    case 11:
                        LOGGER.debug("Commit tick reached");
                        return [4 /*yield*/, this.commitOffset()];
                    case 12:
                        _b.sent();
                        newTask = new aio_1.Interval(this.commitInterval).tick();
                        taskMap.set(newTask, "commit_tick");
                        return [3 /*break*/, 14];
                    case 13: throw new Error("Unexpected task name: ".concat(taskName));
                    case 14: return [4 /*yield*/, this.drainSlotStatus()];
                    case 15:
                        _b.sent();
                        return [3 /*break*/, 3];
                    case 16:
                        LOGGER.debug("Fumarole runtime exiting");
                        return [2 /*return*/];
                }
            });
        });
    };
    return AsyncioFumeDragonsmouthRuntime;
}());
exports.AsyncioFumeDragonsmouthRuntime = AsyncioFumeDragonsmouthRuntime;
var GrpcSlotDownloader = /** @class */ (function () {
    function GrpcSlotDownloader(client) {
        this.client = client;
    }
    GrpcSlotDownloader.prototype.runDownload = function (subscribeRequest, spec) {
        return __awaiter(this, void 0, void 0, function () {
            var downloadTask;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        downloadTask = new GrpcDownloadBlockTaskRun(spec.downloadRequest, this.client, {
                            accounts: subscribeRequest.accounts,
                            transactions: subscribeRequest.transactions,
                            entries: subscribeRequest.entry,
                            blocksMeta: subscribeRequest.blocksMeta,
                        }, spec.dragonsmouthOutlet);
                        LOGGER.debug("Running download task for slot ".concat(spec.downloadRequest.slot));
                        return [4 /*yield*/, downloadTask.run()];
                    case 1: return [2 /*return*/, _a.sent()];
                }
            });
        });
    };
    return GrpcSlotDownloader;
}());
exports.GrpcSlotDownloader = GrpcSlotDownloader;
var GrpcDownloadBlockTaskRun = /** @class */ (function () {
    function GrpcDownloadBlockTaskRun(downloadRequest, client, filters, dragonsmouthOutlet) {
        this.downloadRequest = downloadRequest;
        this.client = client;
        this.filters = filters;
        this.dragonsmouthOutlet = dragonsmouthOutlet;
    }
    GrpcDownloadBlockTaskRun.prototype.mapTonicErrorCodeToDownloadBlockError = function (error) {
        switch (error.code) {
            case grpc_js_1.status.NOT_FOUND:
                return {
                    kind: "BlockShardNotFound",
                    message: "Block shard not found",
                };
            case grpc_js_1.status.UNAVAILABLE:
                return {
                    kind: "Disconnected",
                    message: "Disconnected",
                };
            case grpc_js_1.status.INTERNAL:
            case grpc_js_1.status.ABORTED:
            case grpc_js_1.status.DATA_LOSS:
            case grpc_js_1.status.RESOURCE_EXHAUSTED:
            case grpc_js_1.status.UNKNOWN:
            case grpc_js_1.status.CANCELLED:
            case grpc_js_1.status.DEADLINE_EXCEEDED:
                return {
                    kind: "FailedDownload",
                    message: "Failed download",
                };
            case grpc_js_1.status.INVALID_ARGUMENT:
                throw new Error("Invalid argument");
            default:
                return {
                    kind: "Fatal",
                    message: "Unknown error: ".concat(error.code),
                };
        }
    };
    GrpcDownloadBlockTaskRun.prototype.run = function () {
        return __awaiter(this, void 0, void 0, function () {
            var request, totalEventDownloaded_1, stream_1;
            var _this = this;
            return __generator(this, function (_a) {
                request = {
                    blockchainId: this.downloadRequest.blockchainId,
                    blockUid: this.downloadRequest.blockUid,
                    shardIdx: 0,
                    blockFilters: this.filters,
                };
                try {
                    LOGGER.debug("Requesting download for block ".concat(Buffer.from(this.downloadRequest.blockUid).toString("hex"), " at slot ").concat(this.downloadRequest.slot));
                    totalEventDownloaded_1 = 0;
                    stream_1 = this.client.downloadBlock(request);
                    return [2 /*return*/, new Promise(function (resolve, reject) {
                            stream_1.on("data", function (data) { return __awaiter(_this, void 0, void 0, function () {
                                var kind, _a, update, error_2;
                                return __generator(this, function (_b) {
                                    switch (_b.label) {
                                        case 0:
                                            kind = Object.keys(data).find(function (k) { return data[k] !== undefined && k !== "response"; });
                                            if (!kind)
                                                return [2 /*return*/];
                                            _a = kind;
                                            switch (_a) {
                                                case "update": return [3 /*break*/, 1];
                                                case "blockShardDownloadFinish": return [3 /*break*/, 6];
                                            }
                                            return [3 /*break*/, 7];
                                        case 1:
                                            update = data.update;
                                            if (!update)
                                                throw new Error("Update is null");
                                            totalEventDownloaded_1++;
                                            _b.label = 2;
                                        case 2:
                                            _b.trys.push([2, 4, , 5]);
                                            return [4 /*yield*/, this.dragonsmouthOutlet.put(update)];
                                        case 3:
                                            _b.sent();
                                            return [3 /*break*/, 5];
                                        case 4:
                                            error_2 = _b.sent();
                                            if (error_2.message === "Queue shutdown") {
                                                LOGGER.error("Dragonsmouth outlet is disconnected");
                                                resolve({
                                                    kind: "Err",
                                                    slot: this.downloadRequest.slot,
                                                    err: {
                                                        kind: "OutletDisconnected",
                                                        message: "Outlet disconnected",
                                                    },
                                                });
                                            }
                                            return [3 /*break*/, 5];
                                        case 5: return [3 /*break*/, 8];
                                        case 6:
                                            LOGGER.debug("Download finished for block ".concat(Buffer.from(this.downloadRequest.blockUid).toString("hex"), " at slot ").concat(this.downloadRequest.slot));
                                            resolve({
                                                kind: "Ok",
                                                completed: {
                                                    slot: this.downloadRequest.slot,
                                                    blockUid: this.downloadRequest.blockUid,
                                                    shardIdx: 0,
                                                    totalEventDownloaded: totalEventDownloaded_1,
                                                },
                                            });
                                            return [3 /*break*/, 8];
                                        case 7:
                                            reject(new Error("Unexpected response kind: ".concat(kind)));
                                            _b.label = 8;
                                        case 8: return [2 /*return*/];
                                    }
                                });
                            }); });
                            stream_1.on("error", function (error) {
                                LOGGER.error("Download block error: ".concat(error));
                                resolve({
                                    kind: "Err",
                                    slot: _this.downloadRequest.slot,
                                    err: _this.mapTonicErrorCodeToDownloadBlockError(error),
                                });
                            });
                            stream_1.on("end", function () {
                                resolve({
                                    kind: "Err",
                                    slot: _this.downloadRequest.slot,
                                    err: {
                                        kind: "FailedDownload",
                                        message: "Failed download",
                                    },
                                });
                            });
                        })];
                }
                catch (error) {
                    LOGGER.error("Download block error: ".concat(error));
                    return [2 /*return*/, {
                            kind: "Err",
                            slot: this.downloadRequest.slot,
                            err: this.mapTonicErrorCodeToDownloadBlockError(error),
                        }];
                }
                return [2 /*return*/];
            });
        });
    };
    return GrpcDownloadBlockTaskRun;
}());
exports.GrpcDownloadBlockTaskRun = GrpcDownloadBlockTaskRun;
