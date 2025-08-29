import { ClientReadableStream } from "@grpc/grpc-js";
import { Observable, share } from "rxjs";



export function makeObservable<T>(clientReadableStream: ClientReadableStream<T>): Observable<T> {
    return new Observable<T>((subscriber) => {
        clientReadableStream.on("data", (data: T) => {
            subscriber.next(data);
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