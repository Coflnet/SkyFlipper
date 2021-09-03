FROM mcr.microsoft.com/dotnet/sdk:5.0 as build
WORKDIR /build
RUN echo "revision 35277e34a6d9b89b3b062bfc3f4bd92d80a2b7a5"
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

ENTRYPOINT ["dotnet", "SkyFlipper.dll"]

VOLUME /data
