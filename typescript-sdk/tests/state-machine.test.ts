import { CommitmentLevel } from "../src/grpc/geyser";
import { BlockchainEvent } from "../src/grpc/fumarole";
import {
  FumaroleSM,
  DEFAULT_SLOT_MEMORY_RETENTION,
  SlotDownloadState,
} from "../src/runtime/state-machine";
import { randomBytes } from "crypto";

function randomBlockchainEvent(
  offset: bigint,
  slot: bigint,
  commitment_level: CommitmentLevel,
): BlockchainEvent {
  const nilBlockchainId = new Uint8Array(16); // UUID.nil()
  const randomBlockUid = randomBytes(16); // UUID.v4()

  return {
    offset,
    blockchainId: nilBlockchainId,
    blockUid: randomBlockUid,
    numShards: 1,
    slot,
    parentSlot: undefined,
    commitmentLevel: commitment_level as number,
    blockchainShardId: 0,
    deadError: undefined,
  };
}

describe("FumaroleSM", () => {
  describe("happy path", () => {
    it("should handle basic flow correctly", () => {
      const sm = new FumaroleSM(0n, DEFAULT_SLOT_MEMORY_RETENTION);

      const event = randomBlockchainEvent(1n, 1n, CommitmentLevel.PROCESSED);
      sm.queueBlockchainEvent([event]);

      // Slot status should not be available, since we didn't download it yet
      const downloadReq = sm.popSlotToDownload();
      expect(downloadReq).toBeDefined();
      expect(downloadReq?.slot).toBe(1n);

      expect(sm.popSlotToDownload()).toBeNull();
      expect(sm.popNextSlotStatus()).toBeNull();

      const downloadState = sm.makeSlotDownloadProgress(1n, 0);
      expect(downloadState).toBe(SlotDownloadState.Done);

      const status = sm.popNextSlotStatus();
      expect(status).toBeDefined();
      expect(status?.slot).toBe(1n);
      expect(status?.commitmentLevel).toBe(CommitmentLevel.PROCESSED);
      if (status) {
        sm.markEventAsProcessed(status.sessionSequence);
      }

      // All subsequent commitment level should be available right away
      const event2 = {
        ...event,
        offset: event.offset + 1n,
        commitmentLevel: CommitmentLevel.CONFIRMED as number,
      };
      sm.queueBlockchainEvent([event2]);

      // It should not cause new slot download request
      expect(sm.popSlotToDownload()).toBeNull();

      const status2 = sm.popNextSlotStatus();
      expect(status2).toBeDefined();
      expect(status2?.slot).toBe(1n);
      expect(status2?.commitmentLevel).toBe(CommitmentLevel.CONFIRMED);
      if (status2) {
        sm.markEventAsProcessed(status2.sessionSequence);
      }

      expect(sm.committableOffset).toBe(event2.offset);
    });
  });

  describe("slot status deduplication", () => {
    it("should deduplicate slot status", () => {
      const sm = new FumaroleSM(0n, DEFAULT_SLOT_MEMORY_RETENTION);

      const event = randomBlockchainEvent(1n, 1n, CommitmentLevel.PROCESSED);
      sm.queueBlockchainEvent([event]);

      // Slot status should not be available, since we didn't download it yet
      expect(sm.popNextSlotStatus()).toBeNull();

      const downloadReq = sm.popSlotToDownload();
      expect(downloadReq).toBeDefined();
      expect(downloadReq?.slot).toBe(1n);

      expect(sm.popSlotToDownload()).toBeNull();

      sm.makeSlotDownloadProgress(1n, 0);

      const status = sm.popNextSlotStatus();
      expect(status).toBeDefined();
      expect(status?.slot).toBe(1n);
      expect(status?.commitmentLevel).toBe(CommitmentLevel.PROCESSED);

      // Putting the same event back should be ignored
      sm.queueBlockchainEvent([event]);

      expect(sm.popSlotToDownload()).toBeNull();
      expect(sm.popNextSlotStatus()).toBeNull();
    });
  });

  describe("minimum commitment level", () => {
    it("should handle min commitment level correctly", () => {
      const sm = new FumaroleSM(0n, DEFAULT_SLOT_MEMORY_RETENTION);

      const event = randomBlockchainEvent(1n, 1n, CommitmentLevel.PROCESSED);
      sm.queueBlockchainEvent([event]);

      // Slot status should not be available, since we didn't download it yet
      expect(sm.popNextSlotStatus()).toBeNull();

      // Use finalized commitment level here
      const downloadReq = sm.popSlotToDownload(CommitmentLevel.FINALIZED);
      expect(downloadReq).toBeNull();

      expect(sm.popSlotToDownload()).toBeNull();

      // It should not cause the slot status to be available here even if we have a finalized commitment level filtered out before
      const status = sm.popNextSlotStatus();
      expect(status).toBeDefined();
      expect(status?.slot).toBe(1n);
      expect(status?.commitmentLevel).toBe(CommitmentLevel.PROCESSED);
    });
  });
});
