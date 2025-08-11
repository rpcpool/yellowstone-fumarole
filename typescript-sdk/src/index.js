"use strict";
var __assign = (this && this.__assign) || function () {
    __assign = Object.assign || function(t) {
        for (var s, i = 1, n = arguments.length; i < n; i++) {
            s = arguments[i];
            for (var p in s) if (Object.prototype.hasOwnProperty.call(s, p))
                t[p] = s[p];
        }
        return t;
    };
    return __assign.apply(this, arguments);
};
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
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP = exports.DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT = exports.DEFAULT_COMMIT_INTERVAL = exports.DEFAULT_DRAGONSMOUTH_CAPACITY = exports.FumaroleConfig = exports.FumaroleClient = void 0;
var grpc_js_1 = require("@grpc/grpc-js");
var config_1 = require("./config/config");
Object.defineProperty(exports, "FumaroleConfig", { enumerable: true, get: function () { return config_1.FumaroleConfig; } });
var connectivity_1 = require("./connectivity");
var types_1 = require("./types");
Object.defineProperty(exports, "DEFAULT_DRAGONSMOUTH_CAPACITY", { enumerable: true, get: function () { return types_1.DEFAULT_DRAGONSMOUTH_CAPACITY; } });
Object.defineProperty(exports, "DEFAULT_COMMIT_INTERVAL", { enumerable: true, get: function () { return types_1.DEFAULT_COMMIT_INTERVAL; } });
Object.defineProperty(exports, "DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT", { enumerable: true, get: function () { return types_1.DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT; } });
Object.defineProperty(exports, "DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP", { enumerable: true, get: function () { return types_1.DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP; } });
var FumaroleClient = /** @class */ (function () {
    function FumaroleClient(connector, stub) {
        this.connector = connector;
        this.stub = stub;
    }
    FumaroleClient.connect = function (config) {
        return __awaiter(this, void 0, void 0, function () {
            var endpoint, connector, client, methods;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        endpoint = config.endpoint;
                        connector = new connectivity_1.FumaroleGrpcConnector(config, endpoint);
                        FumaroleClient.logger.debug("Connecting to ".concat(endpoint));
                        FumaroleClient.logger.debug("Connection config:", {
                            endpoint: config.endpoint,
                            xToken: config.xToken ? "***" : "none",
                            maxDecodingMessageSizeBytes: config.maxDecodingMessageSizeBytes,
                        });
                        return [4 /*yield*/, connector.connect()];
                    case 1:
                        client = _a.sent();
                        FumaroleClient.logger.debug("Connected to ".concat(endpoint, ", testing stub..."));
                        // Wait for client to be ready
                        return [4 /*yield*/, new Promise(function (resolve, reject) {
                                var deadline = new Date().getTime() + 5000; // 5 second timeout
                                client.waitForReady(deadline, function (error) {
                                    if (error) {
                                        FumaroleClient.logger.error("Client failed to become ready:", error);
                                        reject(error);
                                    }
                                    else {
                                        FumaroleClient.logger.debug("Client is ready");
                                        resolve(undefined);
                                    }
                                });
                            })];
                    case 2:
                        // Wait for client to be ready
                        _a.sent();
                        // Verify client methods
                        if (!client || typeof client.listConsumerGroups !== "function") {
                            methods = client
                                ? Object.getOwnPropertyNames(Object.getPrototypeOf(client))
                                : [];
                            FumaroleClient.logger.error("Available methods:", methods);
                            throw new Error("gRPC client or listConsumerGroups method not available");
                        }
                        FumaroleClient.logger.debug("gRPC client initialized successfully");
                        return [2 /*return*/, new FumaroleClient(connector, client)];
                }
            });
        });
    };
    FumaroleClient.prototype.version = function () {
        return __awaiter(this, void 0, void 0, function () {
            var request;
            var _this = this;
            return __generator(this, function (_a) {
                FumaroleClient.logger.debug("Sending version request");
                request = {};
                return [2 /*return*/, new Promise(function (resolve, reject) {
                        _this.stub.version(request, function (error, response) {
                            if (error) {
                                FumaroleClient.logger.error("Version request failed:", error);
                                reject(error);
                            }
                            else {
                                FumaroleClient.logger.debug("Version response:", response);
                                resolve(response);
                            }
                        });
                    })];
            });
        });
    };
    FumaroleClient.prototype.dragonsmouthSubscribe = function (consumerGroupName, request) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                return [2 /*return*/, this.dragonsmouthSubscribeWithConfig(consumerGroupName, request, {})];
            });
        });
    };
    FumaroleClient.prototype.dragonsmouthSubscribeWithConfig = function (consumerGroupName, request, config) {
        return __awaiter(this, void 0, void 0, function () {
            var finalConfig, dragonsmouthOutlet, fumeControlPlaneQ, initialJoin, initialJoinCommand, controlPlaneStream, subscribeRequestQueue, fumeControlPlaneRxQ, controlPlaneSourceTask, controlResponse, init, lastCommittedOffsetStr, lastCommittedOffset, dataPlaneClient, runtimeTask;
            var _this = this;
            var _a;
            return __generator(this, function (_b) {
                switch (_b.label) {
                    case 0:
                        finalConfig = __assign({ concurrentDownloadLimit: types_1.DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP, commitInterval: types_1.DEFAULT_COMMIT_INTERVAL, maxFailedSlotDownloadAttempt: types_1.DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT, dataChannelCapacity: types_1.DEFAULT_DRAGONSMOUTH_CAPACITY, gcInterval: types_1.DEFAULT_GC_INTERVAL, slotMemoryRetention: types_1.DEFAULT_SLOT_MEMORY_RETENTION }, config);
                        dragonsmouthOutlet = new types_1.AsyncQueue(finalConfig.dataChannelCapacity);
                        fumeControlPlaneQ = new types_1.AsyncQueue(100);
                        initialJoin = { consumerGroupName: consumerGroupName };
                        initialJoinCommand = { initialJoin: initialJoin };
                        return [4 /*yield*/, fumeControlPlaneQ.put(initialJoinCommand)];
                    case 1:
                        _b.sent();
                        FumaroleClient.logger.debug("Sent initial join command: ".concat(JSON.stringify(initialJoinCommand)));
                        controlPlaneStream = this.stub.subscribe();
                        subscribeRequestQueue = new types_1.AsyncQueue(100);
                        fumeControlPlaneRxQ = new types_1.AsyncQueue(100);
                        controlPlaneSourceTask = (function () { return __awaiter(_this, void 0, void 0, function () {
                            var _a, controlPlaneStream_1, controlPlaneStream_1_1, update, e_1_1, error_1;
                            var _b, e_1, _c, _d;
                            return __generator(this, function (_e) {
                                switch (_e.label) {
                                    case 0:
                                        _e.trys.push([0, 14, , 15]);
                                        _e.label = 1;
                                    case 1:
                                        _e.trys.push([1, 7, 8, 13]);
                                        _a = true, controlPlaneStream_1 = __asyncValues(controlPlaneStream);
                                        _e.label = 2;
                                    case 2: return [4 /*yield*/, controlPlaneStream_1.next()];
                                    case 3:
                                        if (!(controlPlaneStream_1_1 = _e.sent(), _b = controlPlaneStream_1_1.done, !_b)) return [3 /*break*/, 6];
                                        _d = controlPlaneStream_1_1.value;
                                        _a = false;
                                        update = _d;
                                        return [4 /*yield*/, fumeControlPlaneRxQ.put(update)];
                                    case 4:
                                        _e.sent();
                                        _e.label = 5;
                                    case 5:
                                        _a = true;
                                        return [3 /*break*/, 2];
                                    case 6: return [3 /*break*/, 13];
                                    case 7:
                                        e_1_1 = _e.sent();
                                        e_1 = { error: e_1_1 };
                                        return [3 /*break*/, 13];
                                    case 8:
                                        _e.trys.push([8, , 11, 12]);
                                        if (!(!_a && !_b && (_c = controlPlaneStream_1.return))) return [3 /*break*/, 10];
                                        return [4 /*yield*/, _c.call(controlPlaneStream_1)];
                                    case 9:
                                        _e.sent();
                                        _e.label = 10;
                                    case 10: return [3 /*break*/, 12];
                                    case 11:
                                        if (e_1) throw e_1.error;
                                        return [7 /*endfinally*/];
                                    case 12: return [7 /*endfinally*/];
                                    case 13: return [3 /*break*/, 15];
                                    case 14:
                                        error_1 = _e.sent();
                                        if (error_1.code !== "CANCELLED") {
                                            throw error_1;
                                        }
                                        return [3 /*break*/, 15];
                                    case 15: return [2 /*return*/];
                                }
                            });
                        }); })();
                        return [4 /*yield*/, fumeControlPlaneRxQ.get()];
                    case 2:
                        controlResponse = (_b.sent());
                        init = controlResponse.init;
                        if (!init) {
                            throw new Error("Unexpected initial response: ".concat(JSON.stringify(controlResponse)));
                        }
                        FumaroleClient.logger.debug("Control response: ".concat(JSON.stringify(controlResponse)));
                        lastCommittedOffsetStr = (_a = init.lastCommittedOffsets) === null || _a === void 0 ? void 0 : _a[0];
                        if (!lastCommittedOffsetStr) {
                            throw new Error("No last committed offset");
                        }
                        lastCommittedOffset = BigInt(lastCommittedOffsetStr);
                        return [4 /*yield*/, this.connector.connect()];
                    case 3:
                        dataPlaneClient = _b.sent();
                        runtimeTask = this.startRuntime(subscribeRequestQueue, fumeControlPlaneQ, fumeControlPlaneRxQ, dragonsmouthOutlet, request, consumerGroupName, lastCommittedOffset, finalConfig, dataPlaneClient);
                        FumaroleClient.logger.debug("Fumarole handle created: ".concat(runtimeTask));
                        return [2 /*return*/, {
                                sink: subscribeRequestQueue,
                                source: dragonsmouthOutlet,
                                fumaroleHandle: runtimeTask,
                            }];
                }
            });
        });
    };
    FumaroleClient.prototype.startRuntime = function (subscribeRequestQueue, controlPlaneTxQ, controlPlaneRxQ, dragonsmouthOutlet, request, consumerGroupName, lastCommittedOffset, config, dataPlaneClient) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                // Implementation of runtime task here
                // This would be equivalent to AsyncioFumeDragonsmouthRuntime in Python
                // For brevity, this is a placeholder implementation
                return [2 /*return*/, Promise.resolve()];
            });
        });
    };
    FumaroleClient.prototype.listConsumerGroups = function () {
        return __awaiter(this, void 0, void 0, function () {
            var request, metadata;
            var _this = this;
            return __generator(this, function (_a) {
                if (!this.stub) {
                    throw new Error("gRPC stub not initialized");
                }
                if (!this.stub.listConsumerGroups) {
                    throw new Error("listConsumerGroups method not available on stub");
                }
                FumaroleClient.logger.debug("Preparing listConsumerGroups request");
                request = {};
                metadata = new grpc_js_1.Metadata();
                return [2 /*return*/, new Promise(function (resolve, reject) {
                        var hasResponded = false;
                        var timeout = setTimeout(function () {
                            if (!hasResponded) {
                                FumaroleClient.logger.error("ListConsumerGroups timeout after 30s");
                                if (call) {
                                    try {
                                        call.cancel();
                                    }
                                    catch (e) {
                                        FumaroleClient.logger.error("Error cancelling call:", e);
                                    }
                                }
                                reject(new Error("gRPC call timed out after 30 seconds"));
                            }
                        }, 30000); // 30 second timeout
                        var call;
                        try {
                            FumaroleClient.logger.debug("Starting gRPC listConsumerGroups call");
                            call = _this.stub.listConsumerGroups(request, metadata, {
                                deadline: Date.now() + 30000, // 30 second deadline
                            }, function (error, response) {
                                var _a;
                                hasResponded = true;
                                clearTimeout(timeout);
                                if (error) {
                                    var errorDetails = {
                                        code: error.code,
                                        details: error.details,
                                        metadata: (_a = error.metadata) === null || _a === void 0 ? void 0 : _a.getMap(),
                                        stack: error.stack,
                                        message: error.message,
                                        name: error.name,
                                    };
                                    FumaroleClient.logger.error("ListConsumerGroups error:", errorDetails);
                                    reject(error);
                                }
                                else {
                                    FumaroleClient.logger.debug("ListConsumerGroups success - Response:", JSON.stringify(response, null, 2));
                                    resolve(response);
                                }
                            });
                            // Monitor call state
                            if (call) {
                                call.on("metadata", function (metadata) {
                                    FumaroleClient.logger.debug("Received metadata:", metadata.getMap());
                                });
                                call.on("status", function (status) {
                                    FumaroleClient.logger.debug("Call status:", status);
                                });
                                call.on("error", function (error) {
                                    FumaroleClient.logger.error("Call stream error:", error);
                                    if (!hasResponded) {
                                        hasResponded = true;
                                        clearTimeout(timeout);
                                        reject(error);
                                    }
                                });
                            }
                            else {
                                FumaroleClient.logger.error("Failed to create gRPC call object");
                                hasResponded = true;
                                clearTimeout(timeout);
                                reject(new Error("Failed to create gRPC call"));
                            }
                        }
                        catch (setupError) {
                            hasResponded = true;
                            clearTimeout(timeout);
                            FumaroleClient.logger.error("Error setting up gRPC call:", setupError);
                            reject(setupError);
                        }
                    })];
            });
        });
    };
    FumaroleClient.prototype.getConsumerGroupInfo = function (consumerGroupName) {
        return __awaiter(this, void 0, void 0, function () {
            var request;
            var _this = this;
            return __generator(this, function (_a) {
                FumaroleClient.logger.debug("Sending getConsumerGroupInfo request:", consumerGroupName);
                request = { consumerGroupName: consumerGroupName };
                return [2 /*return*/, new Promise(function (resolve, reject) {
                        _this.stub.getConsumerGroupInfo(request, function (error, response) {
                            if (error) {
                                if (error.code === 14) {
                                    // grpc.status.NOT_FOUND
                                    FumaroleClient.logger.debug("Consumer group not found:", consumerGroupName);
                                    resolve(null);
                                }
                                else {
                                    FumaroleClient.logger.error("GetConsumerGroupInfo error:", error);
                                    reject(error);
                                }
                            }
                            else {
                                FumaroleClient.logger.debug("GetConsumerGroupInfo response:", response);
                                resolve(response);
                            }
                        });
                    })];
            });
        });
    };
    FumaroleClient.prototype.deleteConsumerGroup = function (consumerGroupName) {
        return __awaiter(this, void 0, void 0, function () {
            var request;
            var _this = this;
            return __generator(this, function (_a) {
                FumaroleClient.logger.debug("Sending deleteConsumerGroup request:", consumerGroupName);
                request = { consumerGroupName: consumerGroupName };
                return [2 /*return*/, new Promise(function (resolve, reject) {
                        _this.stub.deleteConsumerGroup(request, function (error, response) {
                            if (error) {
                                FumaroleClient.logger.error("DeleteConsumerGroup error:", error);
                                reject(error);
                            }
                            else {
                                FumaroleClient.logger.debug("DeleteConsumerGroup response:", response);
                                resolve(response);
                            }
                        });
                    })];
            });
        });
    };
    FumaroleClient.prototype.deleteAllConsumerGroups = function () {
        return __awaiter(this, void 0, void 0, function () {
            var response, deletePromises, results, failures;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.listConsumerGroups()];
                    case 1:
                        response = _a.sent();
                        deletePromises = response.consumerGroups.map(function (group) {
                            return _this.deleteConsumerGroup(group.consumerGroupName);
                        });
                        return [4 /*yield*/, Promise.all(deletePromises)];
                    case 2:
                        results = _a.sent();
                        failures = results.filter(function (result) { return !result.success; });
                        if (failures.length > 0) {
                            throw new Error("Failed to delete some consumer groups: ".concat(JSON.stringify(failures)));
                        }
                        return [2 /*return*/];
                }
            });
        });
    };
    FumaroleClient.prototype.createConsumerGroup = function (request) {
        return __awaiter(this, void 0, void 0, function () {
            var _this = this;
            return __generator(this, function (_a) {
                FumaroleClient.logger.debug("Sending createConsumerGroup request:", request);
                return [2 /*return*/, new Promise(function (resolve, reject) {
                        _this.stub.createConsumerGroup(request, function (error, response) {
                            if (error) {
                                FumaroleClient.logger.error("CreateConsumerGroup error:", error);
                                reject(error);
                            }
                            else {
                                FumaroleClient.logger.debug("CreateConsumerGroup response:", response);
                                resolve(response);
                            }
                        });
                    })];
            });
        });
    };
    FumaroleClient.logger = console;
    return FumaroleClient;
}());
exports.FumaroleClient = FumaroleClient;
