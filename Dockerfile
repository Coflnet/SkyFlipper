FROM mcr.microsoft.com/dotnet/sdk:5.0 as build
WORKDIR /build
RUN echo "revision 37a7e96bdc9cb7d0c6e9a1e280e37297a975a118b"
RUN git clone --depth=1 -b separation https://github.com/Coflnet/HypixelSkyblock.git dev
WORKDIR /build/sky
COPY SkyFlipper.csproj SkyFlipper.csproj
RUN dotnet restore
COPY . .
RUN dotnet publish -c release

FROM mcr.microsoft.com/dotnet/aspnet:5.0
WORKDIR /app

COPY --from=build /build/sky/bin/release/net5.0/publish/ .
RUN mkdir -p ah/files
ENV ASPNETCORE_URLS=http://+:8000;http://+:80

ENTRYPOINT ["dotnet", "SkyFlipper.dll", "--hostBuilder:reloadConfigOnChange=false"]

VOLUME /data
