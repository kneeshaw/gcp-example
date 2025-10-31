declare module "react-plotly.js" {
  import * as React from "react";
  export interface PlotParams {
    data: any[];
    layout?: any;
    config?: any;
    style?: React.CSSProperties;
    className?: string;
    useResizeHandler?: boolean;
    onInitialized?: (figure: any, graphDiv: any) => void;
    onUpdate?: (figure: any, graphDiv: any) => void;
    divId?: string;
  }
  const Plot: React.FC<PlotParams>;
  export default Plot;
}
