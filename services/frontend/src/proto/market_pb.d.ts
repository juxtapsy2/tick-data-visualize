import * as jspb from 'google-protobuf'



export class MarketData extends jspb.Message {
  getTimestamp(): number;
  setTimestamp(value: number): MarketData;

  getVn30Value(): number;
  setVn30Value(value: number): MarketData;

  getHnxValue(): number;
  setHnxValue(value: number): MarketData;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): MarketData.AsObject;
  static toObject(includeInstance: boolean, msg: MarketData): MarketData.AsObject;
  static serializeBinaryToWriter(message: MarketData, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): MarketData;
  static deserializeBinaryFromReader(message: MarketData, reader: jspb.BinaryReader): MarketData;
}

export namespace MarketData {
  export type AsObject = {
    timestamp: number;
    vn30Value: number;
    hnxValue: number;
  };
}

export class HistoricalRequest extends jspb.Message {
  getDate(): string;
  setDate(value: string): HistoricalRequest;

  getFromTimestamp(): number;
  setFromTimestamp(value: number): HistoricalRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): HistoricalRequest.AsObject;
  static toObject(includeInstance: boolean, msg: HistoricalRequest): HistoricalRequest.AsObject;
  static serializeBinaryToWriter(message: HistoricalRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): HistoricalRequest;
  static deserializeBinaryFromReader(message: HistoricalRequest, reader: jspb.BinaryReader): HistoricalRequest;
}

export namespace HistoricalRequest {
  export type AsObject = {
    date: string;
    fromTimestamp: number;
  };
}

export class HistoricalResponse extends jspb.Message {
  getDataList(): Array<MarketData>;
  setDataList(value: Array<MarketData>): HistoricalResponse;
  clearDataList(): HistoricalResponse;
  addData(value?: MarketData, index?: number): MarketData;

  getCount(): number;
  setCount(value: number): HistoricalResponse;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): HistoricalResponse.AsObject;
  static toObject(includeInstance: boolean, msg: HistoricalResponse): HistoricalResponse.AsObject;
  static serializeBinaryToWriter(message: HistoricalResponse, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): HistoricalResponse;
  static deserializeBinaryFromReader(message: HistoricalResponse, reader: jspb.BinaryReader): HistoricalResponse;
}

export namespace HistoricalResponse {
  export type AsObject = {
    dataList: Array<MarketData.AsObject>;
    count: number;
  };
}

export class StreamRequest extends jspb.Message {
  getFromTimestamp(): number;
  setFromTimestamp(value: number): StreamRequest;

  serializeBinary(): Uint8Array;
  toObject(includeInstance?: boolean): StreamRequest.AsObject;
  static toObject(includeInstance: boolean, msg: StreamRequest): StreamRequest.AsObject;
  static serializeBinaryToWriter(message: StreamRequest, writer: jspb.BinaryWriter): void;
  static deserializeBinary(bytes: Uint8Array): StreamRequest;
  static deserializeBinaryFromReader(message: StreamRequest, reader: jspb.BinaryReader): StreamRequest;
}

export namespace StreamRequest {
  export type AsObject = {
    fromTimestamp: number;
  };
}

