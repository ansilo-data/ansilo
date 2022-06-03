import * as React from "react";
import { EntitySchema, EntitySchemaVersion } from "./catalog.slice";
import EntityIcon from "@mui/icons-material/TableChartOutlined";
import Paper from "@mui/material/Paper";
import Box from "@mui/material/Box";
import Typography from "@mui/material/Typography";
import { versionLabel } from "../../util/versionLabel";
import Divider from "@mui/material/Divider";
import EntityAttributesTable from "./EntityAttributesTable";

interface Props {
  entity: EntitySchema;
  version: EntitySchemaVersion;
}

export default function EntityVersionDetails(props: Props) {
  return (
    <Box>
      <Box sx={{ display: "flex", justifyContent: "space-between" }}>
        <Typography variant="h4">
          <EntityIcon /> {props.entity.name}
        </Typography>
        <Typography variant="subtitle1" sx={{color: 'text.secondary'}}>
          {versionLabel(props.version.version)}
        </Typography>
      </Box>
      <Divider sx={{my: 4}} />
      <Typography variant="body1" sx={{my: 4}}>{props.entity.description}</Typography>
      <EntityAttributesTable version={props.version} />
    </Box>
  );
}
