import * as React from "react";
import type { NextPage } from "next";
import { Template } from "../../components/template/Template";
import { CreateJob } from "../../components/jobs/create/CreateJob";

const Page: NextPage = () => {
  return (
    <Template title="Jobs > Create">
      <CreateJob />
    </Template>
  );
};

export default Page;
