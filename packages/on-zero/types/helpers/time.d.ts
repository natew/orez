interface MsFunction {
    (n: number): number;
    seconds: (n: number) => number;
    minutes: (n: number) => number;
    hours: (n: number) => number;
    days: (n: number) => number;
    weeks: (n: number) => number;
}
interface SecondFunction {
    (n: number): number;
    minutes: (n: number) => number;
    hours: (n: number) => number;
    days: (n: number) => number;
    weeks: (n: number) => number;
}
interface MinuteFunction {
    (n: number): number;
    hours: (n: number) => number;
    days: (n: number) => number;
    weeks: (n: number) => number;
}
export declare const time: {
    ms: MsFunction;
    second: SecondFunction;
    minute: MinuteFunction;
};
export {};
//# sourceMappingURL=time.d.ts.map