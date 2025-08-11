"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.FumaroleConfig = void 0;
var yaml = require("js-yaml");
var FumaroleConfig = /** @class */ (function () {
    function FumaroleConfig(options) {
        var _a, _b;
        this.endpoint = options.endpoint;
        this.xToken = options.xToken;
        this.maxDecodingMessageSizeBytes =
            (_a = options.maxDecodingMessageSizeBytes) !== null && _a !== void 0 ? _a : FumaroleConfig.DEFAULT_MAX_DECODING_MESSAGE_SIZE;
        this.xMetadata = (_b = options.xMetadata) !== null && _b !== void 0 ? _b : {};
    }
    FumaroleConfig.fromYaml = function (yamlContent) {
        var _a, _b;
        var data = yaml.load(yamlContent);
        return new FumaroleConfig({
            endpoint: data.endpoint,
            xToken: data["x-token"] || data.x_token,
            maxDecodingMessageSizeBytes: (_a = data.max_decoding_message_size_bytes) !== null && _a !== void 0 ? _a : FumaroleConfig.DEFAULT_MAX_DECODING_MESSAGE_SIZE,
            xMetadata: (_b = data["x-metadata"]) !== null && _b !== void 0 ? _b : {},
        });
    };
    FumaroleConfig.DEFAULT_MAX_DECODING_MESSAGE_SIZE = 512000000;
    return FumaroleConfig;
}());
exports.FumaroleConfig = FumaroleConfig;
