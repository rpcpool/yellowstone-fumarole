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
exports.Queue = void 0;
var Queue = /** @class */ (function () {
    function Queue(maxSize) {
        if (maxSize === void 0) { maxSize = Infinity; }
        this.items = [];
        this.closed = false;
        this.maxSize = maxSize;
    }
    Queue.prototype.put = function (item) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                if (this.closed) {
                    throw new Error("Queue shutdown");
                }
                if (this.items.length >= this.maxSize) {
                    throw new Error("Queue full");
                }
                this.items.push(item);
                return [2 /*return*/];
            });
        });
    };
    Queue.prototype.get = function () {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        if (this.closed && this.items.length === 0) {
                            throw new Error("Queue shutdown");
                        }
                        _a.label = 1;
                    case 1:
                        if (!(this.items.length === 0)) return [3 /*break*/, 3];
                        return [4 /*yield*/, new Promise(function (resolve) { return setTimeout(resolve, 10); })];
                    case 2:
                        _a.sent();
                        return [3 /*break*/, 1];
                    case 3: return [2 /*return*/, this.items.shift()];
                }
            });
        });
    };
    Queue.prototype.isEmpty = function () {
        return this.items.length === 0;
    };
    Queue.prototype.isFull = function () {
        return this.items.length >= this.maxSize;
    };
    Queue.prototype.size = function () {
        return this.items.length;
    };
    Queue.prototype.close = function () {
        this.closed = true;
    };
    Queue.prototype[Symbol.asyncIterator] = function () {
        var _this = this;
        return {
            next: function () { return __awaiter(_this, void 0, void 0, function () {
                var value, error_1;
                return __generator(this, function (_a) {
                    switch (_a.label) {
                        case 0:
                            if (this.closed && this.isEmpty()) {
                                return [2 /*return*/, { done: true, value: undefined }];
                            }
                            _a.label = 1;
                        case 1:
                            _a.trys.push([1, 3, , 4]);
                            return [4 /*yield*/, this.get()];
                        case 2:
                            value = _a.sent();
                            return [2 /*return*/, { done: false, value: value }];
                        case 3:
                            error_1 = _a.sent();
                            if (error_1.message === "Queue shutdown") {
                                return [2 /*return*/, { done: true, value: undefined }];
                            }
                            throw error_1;
                        case 4: return [2 /*return*/];
                    }
                });
            }); },
        };
    };
    return Queue;
}());
exports.Queue = Queue;
