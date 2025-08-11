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
var src_1 = require("../src");
function main() {
    return __awaiter(this, void 0, void 0, function () {
        var config, client, response, _i, _a, group, info, err_1, error_1, error_2;
        return __generator(this, function (_b) {
            switch (_b.label) {
                case 0:
                    _b.trys.push([0, 13, , 14]);
                    config = {
                        endpoint: "https://fra141.nodes.rpcpool.com", // Replace with your Fumarole endpoint
                        xToken: "7b042cd6-ea1e-46af-b46b-653bdce119f6",
                        maxDecodingMessageSizeBytes: 100 * 1024 * 1024, // 100MB max message size
                        xMetadata: {}, // Additional metadata if needed
                    };
                    // Connect to the Fumarole server
                    console.log("Connecting to Fumarole server...");
                    return [4 /*yield*/, src_1.FumaroleClient.connect(config)];
                case 1:
                    client = _b.sent();
                    console.log("Connected successfully");
                    // List all consumer groups
                    console.log("\nFetching consumer groups...");
                    _b.label = 2;
                case 2:
                    _b.trys.push([2, 11, , 12]);
                    console.log("Sending listConsumerGroups request to server...");
                    process.on("unhandledRejection", function (reason, promise) {
                        console.error("Unhandled Rejection at:", promise, "reason:", reason);
                    });
                    return [4 /*yield*/, client.listConsumerGroups().catch(function (error) {
                            console.error("Caught error during listConsumerGroups:", error);
                            if (error.code)
                                console.error("Error code:", error.code);
                            if (error.details)
                                console.error("Error details:", error.details);
                            if (error.metadata)
                                console.error("Error metadata:", error.metadata);
                            if (error.stack)
                                console.error("Error stack:", error.stack);
                            throw error;
                        })];
                case 3:
                    response = _b.sent();
                    console.log("\n=== ListConsumerGroups Response ===");
                    console.log(JSON.stringify(response, null, 2));
                    console.log("=====================================\n");
                    if (!(!response.consumerGroups || response.consumerGroups.length === 0)) return [3 /*break*/, 4];
                    console.log("No consumer groups found on server");
                    return [3 /*break*/, 10];
                case 4:
                    console.log("Found ".concat(response.consumerGroups.length, " consumer groups. Fetching details...\n"));
                    _i = 0, _a = response.consumerGroups;
                    _b.label = 5;
                case 5:
                    if (!(_i < _a.length)) return [3 /*break*/, 10];
                    group = _a[_i];
                    console.log("=== Consumer Group: ".concat(group.consumerGroupName, " ==="));
                    console.log("Basic info:", JSON.stringify(group, null, 2));
                    _b.label = 6;
                case 6:
                    _b.trys.push([6, 8, , 9]);
                    console.log("\nFetching detailed info for group: ".concat(group.consumerGroupName));
                    return [4 /*yield*/, client.getConsumerGroupInfo(group.consumerGroupName)];
                case 7:
                    info = _b.sent();
                    if (info) {
                        console.log("\nDetailed Group Info:");
                        console.log("Status: Active");
                        console.log("Server Response:", JSON.stringify(info, null, 2));
                    }
                    else {
                        console.log("\nGroup Status: Not found or inactive");
                    }
                    console.log("===============================\n");
                    return [3 /*break*/, 9];
                case 8:
                    err_1 = _b.sent();
                    console.error("\nError fetching group info from server: ".concat(err_1 instanceof Error ? err_1.message : String(err_1)));
                    return [3 /*break*/, 9];
                case 9:
                    _i++;
                    return [3 /*break*/, 5];
                case 10: return [3 /*break*/, 12];
                case 11:
                    error_1 = _b.sent();
                    console.error("Error:", error_1 instanceof Error ? error_1.message : String(error_1));
                    process.exit(1);
                    return [3 /*break*/, 12];
                case 12: return [3 /*break*/, 14];
                case 13:
                    error_2 = _b.sent();
                    console.error("Error:", error_2 instanceof Error ? error_2.message : String(error_2));
                    process.exit(1);
                    return [3 /*break*/, 14];
                case 14: return [2 /*return*/];
            }
        });
    });
}
main().catch(console.error);
