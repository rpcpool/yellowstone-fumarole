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
Object.defineProperty(exports, "__esModule", { value: true });
exports.FumaroleGrpcConnector = void 0;
var grpc_js_1 = require("@grpc/grpc-js");
var fumarole_1 = require("./grpc/fumarole");
var X_TOKEN_HEADER = "x-token";
var TritonAuthMetadataGenerator = /** @class */ (function () {
    function TritonAuthMetadataGenerator(xToken) {
        this.xToken = xToken;
    }
    TritonAuthMetadataGenerator.prototype.generateMetadata = function () {
        var metadata = new grpc_js_1.Metadata();
        metadata.set(X_TOKEN_HEADER, this.xToken);
        return Promise.resolve(metadata);
    };
    return TritonAuthMetadataGenerator;
}());
var MetadataProvider = /** @class */ (function () {
    function MetadataProvider(metadata) {
        var _this = this;
        this.metadata = new grpc_js_1.Metadata();
        Object.entries(metadata).forEach(function (_a) {
            var key = _a[0], value = _a[1];
            _this.metadata.set(key, value);
        });
    }
    MetadataProvider.prototype.getMetadata = function () {
        return Promise.resolve(this.metadata);
    };
    return MetadataProvider;
}());
var FumaroleGrpcConnector = /** @class */ (function () {
    function FumaroleGrpcConnector(config, endpoint) {
        this.config = config;
        this.endpoint = endpoint;
    }
    FumaroleGrpcConnector.prototype.connect = function () {
        return __awaiter(this, arguments, void 0, function (grpcOptions) {
            var options, channelCredentials, insecureXToken, endpointURL, port, address, clientOptions, client, error_1;
            var _this = this;
            if (grpcOptions === void 0) { grpcOptions = {}; }
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        options = __assign({ "grpc.max_receive_message_length": 111111110 }, grpcOptions);
                        endpointURL = new URL(this.endpoint);
                        port = endpointURL.port;
                        if (port === "") {
                            port = endpointURL.protocol === "https:" ? "443" : "80";
                        }
                        address = "".concat(endpointURL.hostname, ":").concat(port);
                        // Handle credentials based on protocol
                        if (endpointURL.protocol === "https:") {
                            channelCredentials = grpc_js_1.credentials.combineChannelCredentials(grpc_js_1.credentials.createSsl(), grpc_js_1.credentials.createFromMetadataGenerator(function (_params, callback) {
                                var metadata = new grpc_js_1.Metadata();
                                if (_this.config.xToken) {
                                    metadata.add("x-token", _this.config.xToken);
                                }
                                if (_this.config.xMetadata) {
                                    Object.entries(_this.config.xMetadata).forEach(function (_a) {
                                        var key = _a[0], value = _a[1];
                                        metadata.add(key, value);
                                    });
                                }
                                callback(null, metadata);
                            }));
                        }
                        else {
                            channelCredentials = grpc_js_1.credentials.createInsecure();
                            if (this.config.xToken) {
                                insecureXToken = this.config.xToken;
                            }
                        }
                        clientOptions = __assign(__assign({}, options), { "grpc.enable_http_proxy": 0, 
                            // Basic keepalive settings
                            "grpc.keepalive_time_ms": 20000, "grpc.keepalive_timeout_ms": 10000, "grpc.http2.min_time_between_pings_ms": 10000, 
                            // Connection settings
                            "grpc.initial_reconnect_backoff_ms": 100, "grpc.max_reconnect_backoff_ms": 3000, "grpc.min_reconnect_backoff_ms": 100, 
                            // Enable retries
                            "grpc.enable_retries": 1, "grpc.service_config": JSON.stringify({
                                methodConfig: [
                                    {
                                        name: [{}], // Apply to all methods
                                        retryPolicy: {
                                            maxAttempts: 5,
                                            initialBackoff: "0.1s",
                                            maxBackoff: "3s",
                                            backoffMultiplier: 2,
                                            retryableStatusCodes: ["UNAVAILABLE", "DEADLINE_EXCEEDED"],
                                        },
                                    },
                                ],
                            }) });
                        client = new fumarole_1.FumaroleClient(address, channelCredentials, clientOptions);
                        _a.label = 1;
                    case 1:
                        _a.trys.push([1, 3, , 4]);
                        return [4 /*yield*/, new Promise(function (resolve, reject) {
                                var deadline = Date.now() + 5000; // 5 second timeout
                                client.waitForReady(deadline, function (err) {
                                    if (err) {
                                        reject(err);
                                    }
                                    else {
                                        resolve();
                                    }
                                });
                            })];
                    case 2:
                        _a.sent();
                        return [3 /*break*/, 4];
                    case 3:
                        error_1 = _a.sent();
                        throw error_1;
                    case 4: return [2 /*return*/, client];
                }
            });
        });
    };
    FumaroleGrpcConnector.logger = console;
    return FumaroleGrpcConnector;
}());
exports.FumaroleGrpcConnector = FumaroleGrpcConnector;
