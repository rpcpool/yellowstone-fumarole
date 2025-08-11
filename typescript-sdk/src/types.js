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
exports.AsyncQueue = exports.DEFAULT_SLOT_MEMORY_RETENTION = exports.DEFAULT_GC_INTERVAL = exports.DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP = exports.DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT = exports.DEFAULT_COMMIT_INTERVAL = exports.DEFAULT_DRAGONSMOUTH_CAPACITY = void 0;
// Constants
exports.DEFAULT_DRAGONSMOUTH_CAPACITY = 10000;
exports.DEFAULT_COMMIT_INTERVAL = 5.0; // seconds
exports.DEFAULT_MAX_SLOT_DOWNLOAD_ATTEMPT = 3;
exports.DEFAULT_CONCURRENT_DOWNLOAD_LIMIT_PER_TCP = 10;
exports.DEFAULT_GC_INTERVAL = 60; // seconds
exports.DEFAULT_SLOT_MEMORY_RETENTION = 300; // seconds
// Generic async queue interface to mimic Python's asyncio.Queue
var AsyncQueue = /** @class */ (function () {
    function AsyncQueue(maxSize) {
        if (maxSize === void 0) { maxSize = 0; }
        this.queue = [];
        this.resolvers = [];
        this.full_resolvers = [];
        this.closed = false;
        this.maxSize = maxSize;
    }
    AsyncQueue.prototype.put = function (item) {
        return __awaiter(this, void 0, void 0, function () {
            var resolver;
            var _this = this;
            return __generator(this, function (_a) {
                if (this.closed) {
                    throw new Error("Queue is closed");
                }
                if (this.maxSize > 0 && this.queue.length >= this.maxSize) {
                    return [2 /*return*/, new Promise(function (resolve) {
                            _this.full_resolvers.push(resolve);
                        })];
                }
                this.queue.push(item);
                resolver = this.resolvers.shift();
                if (resolver) {
                    resolver(this.queue.shift());
                }
                return [2 /*return*/];
            });
        });
    };
    AsyncQueue.prototype.get = function () {
        return __awaiter(this, void 0, void 0, function () {
            var item, full_resolver;
            var _this = this;
            return __generator(this, function (_a) {
                if (this.closed && this.queue.length === 0) {
                    throw new Error("Queue is closed");
                }
                if (this.queue.length === 0) {
                    return [2 /*return*/, new Promise(function (resolve) {
                            _this.resolvers.push(resolve);
                        })];
                }
                item = this.queue.shift();
                full_resolver = this.full_resolvers.shift();
                if (full_resolver) {
                    full_resolver();
                }
                return [2 /*return*/, item];
            });
        });
    };
    AsyncQueue.prototype.close = function () {
        this.closed = true;
        // Resolve all pending gets with an error
        this.resolvers.forEach(function (resolve) {
            resolve(undefined);
        });
        this.resolvers = [];
    };
    return AsyncQueue;
}());
exports.AsyncQueue = AsyncQueue;
