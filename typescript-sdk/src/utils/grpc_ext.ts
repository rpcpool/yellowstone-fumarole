import { ClientReadableStream } from "@grpc/grpc-js";
import { Observable, share } from "rxjs";
import { LOGGER } from "../logging";

export function makeObservable<T>(
  clientReadableStream: ClientReadableStream<T>,
): Observable<T> {
  return new Observable<T>((subscriber) => {
    clientReadableStream.on("data", (data: T) => {
      try {
        subscriber.next(data);
      } catch (e) {
        LOGGER.error(
          `makeObservable -- Error sending data to subscriber: ${e}`,
        );
      }
    });

    clientReadableStream.on("error", (err: Error) => {
      subscriber.error(err);
    });

    clientReadableStream.on("end", () => {
      subscriber.complete();
    });

    return () => {
      console.debug("teardown clientReadStream subscription");
      clientReadableStream.cancel();
    };
  }).pipe(share());
}
