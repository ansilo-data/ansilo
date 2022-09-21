import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../../components/template/Template";
import { ServiceUsers } from "../../../components/governance/service-users/ServiceUsers";

const Page: NextPage = () => {
  return (
    <Template title="Governance > Service Users">
      <ServiceUsers />
    </Template>
  );
};

export default Page;
