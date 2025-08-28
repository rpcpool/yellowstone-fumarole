export class Interval {
  private ms: number;

  constructor(ms: number) {
    this.ms = ms;
  }

  async tick(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, this.ms));
  }
}
