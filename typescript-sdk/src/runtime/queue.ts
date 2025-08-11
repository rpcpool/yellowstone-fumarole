export class Queue<T> {
  private items: T[] = [];
  private maxSize: number;
  private closed: boolean = false;

  constructor(maxSize: number = Infinity) {
    this.maxSize = maxSize;
  }

  async put(item: T): Promise<void> {
    if (this.closed) {
      throw new Error("Queue shutdown");
    }

    if (this.items.length >= this.maxSize) {
      throw new Error("Queue full");
    }

    this.items.push(item);
  }

  async get(): Promise<T> {
    if (this.closed && this.items.length === 0) {
      throw new Error("Queue shutdown");
    }

    // Wait for an item to be available
    while (this.items.length === 0) {
      await new Promise((resolve) => setTimeout(resolve, 10));
    }

    return this.items.shift()!;
  }

  isEmpty(): boolean {
    return this.items.length === 0;
  }

  isFull(): boolean {
    return this.items.length >= this.maxSize;
  }

  size(): number {
    return this.items.length;
  }

  close(): void {
    this.closed = true;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: async (): Promise<IteratorResult<T>> => {
        if (this.closed && this.isEmpty()) {
          return { done: true, value: undefined };
        }

        try {
          const value = await this.get();
          return { done: false, value };
        } catch (error) {
          if (error.message === "Queue shutdown") {
            return { done: true, value: undefined };
          }
          throw error;
        }
      },
    };
  }
}
