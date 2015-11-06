using System;
using Nancy;
using Nancy.Security;
using Nancy.Conventions;

namespace nancySh
{
    public class Application : DefaultNancyBootstrapper
    {
        protected override void ApplicationStartup(Nancy.TinyIoc.TinyIoCContainer container, Nancy.Bootstrapper.IPipelines pipelines)
        {
            Csrf.Enable(pipelines);
            base.ApplicationStartup(container, pipelines);
        }
    }
}

